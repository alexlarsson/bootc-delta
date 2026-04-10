package ocidelta

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/containers/storage"
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ApplyOptions struct {
	DeltaPath      string
	OutputPath     string
	RepoPath       string
	DeltaSource    string
	ContainerStore storage.Store
	TmpDir         string
}

func getDataSource(opts *ApplyOptions, sourceConfigDigest string, log Logger) (deltaDataSource, error) {
	if opts.DeltaSource != "" {
		return &simpleDataSource{tarpatch.NewFilesystemDataSource(opts.DeltaSource)}, nil
	}
	if opts.ContainerStore != nil {
		return resolveContainerStorageDataSource(opts.ContainerStore, sourceConfigDigest, log)
	}
	ds, err := resolveOstreeDataSource(opts.RepoPath, sourceConfigDigest, log)
	if err != nil {
		return nil, err
	}
	return &simpleDataSource{ds}, nil
}

func ApplyDelta(opts ApplyOptions, log Logger) error {
	log.Debug("Applying delta: %s", opts.DeltaPath)
	log.Debug("Output: %s", opts.OutputPath)
	if opts.DeltaSource != "" {
		log.Debug("Delta source: %s", opts.DeltaSource)
	} else if opts.ContainerStore != nil {
		log.Debug("Container storage")
	} else {
		log.Debug("Ostree repo: %s", opts.RepoPath)
	}

	log.Debug("\nParsing delta...")
	delta, err := parseDeltaArtifact(opts.DeltaPath, log)
	if err != nil {
		return err
	}
	defer delta.Close()

	dataSource, err := getDataSource(&opts, delta.sourceConfigDigest, log)
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}
	defer func() {
		_ = dataSource.Close()
		_ = dataSource.Cleanup()
	}()

	// Reconstruct diff_id lookup from image config.
	layerDiffIDs := delta.imageConfig.RootFS.DiffIDs

	writer, err := OpenOCIWriter(opts.OutputPath, opts.TmpDir)
	if err != nil {
		return fmt.Errorf("failed to create output: %w", err)
	}
	defer writer.Close()

	log.Debug("\nWriting oci-layout")
	if err := writer.WriteFile("oci-layout", ociLayoutFileData); err != nil {
		return err
	}

	// Write image config blob (unchanged).
	if err := writeBlob(writer, delta.tarIndex, delta.imageConfigDigest); err != nil {
		return fmt.Errorf("failed to write image config: %w", err)
	}

	// Process each layer in the image manifest.
	// For reconstructed layers we need to remap the digest.
	outputLayers := make([]v1.Descriptor, len(delta.imageManifest.Layers))
	copy(outputLayers, delta.imageManifest.Layers)

	log.Debug("\nProcessing layers...")
	for i, layer := range delta.imageManifest.Layers {
		deltaLayer, inDelta := delta.deltaLayerByTo[layer.Digest]
		if !inDelta {
			// Reused layer: keep original descriptor, no blob written.
			log.Debug("  Layer %s: skipped (not in delta)", layer.Digest.Encoded()[:16])
			continue
		}

		var expectedDiffID digest.Digest
		if i < len(layerDiffIDs) {
			expectedDiffID = layerDiffIDs[i]
		}

		if deltaLayer.MediaType == mediaTypeTarDiff {
			log.Debug("  Layer %s: reconstructing from tar-diff", layer.Digest.Encoded()[:16])
			r, err := delta.GetBlobReader(deltaLayer.Digest)
			if err != nil {
				return fmt.Errorf("failed to read tar-diff for layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
			newDigest, newSize, err := processLayerDiff(opts.TmpDir, log, writer, r, expectedDiffID, dataSource)
			if err != nil {
				return err
			}
			outputLayers[i].Digest = newDigest
			outputLayers[i].Size = newSize
		} else {
			log.Debug("  Layer %s: copying original (%d bytes)", layer.Digest.Encoded()[:16], deltaLayer.Size)
			if err := writeBlob(writer, delta.tarIndex, layer.Digest); err != nil {
				return fmt.Errorf("failed to copy layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
		}
	}

	// Build and write the output image manifest.
	log.Debug("\nWriting output image manifest...")
	outputManifest := delta.imageManifest
	outputManifest.Layers = outputLayers
	outputManifestData, err := json.Marshal(outputManifest)
	if err != nil {
		return fmt.Errorf("failed to marshal output manifest: %w", err)
	}
	outputManifestDigest := computeDigest(outputManifestData)
	if err := writer.WriteFile(blobTarName(outputManifestDigest), outputManifestData); err != nil {
		return err
	}

	// Build and write index.json.
	outputIndex := v1.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: v1.MediaTypeImageIndex,
		Manifests: []v1.Descriptor{{
			MediaType: v1.MediaTypeImageManifest,
			Digest:    outputManifestDigest,
			Size:      int64(len(outputManifestData)),
		}},
	}
	indexData, err := json.Marshal(outputIndex)
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}
	log.Debug("\nWriting index.json")
	if err := writer.WriteFile("index.json", indexData); err != nil {
		return err
	}

	log.Debug("\nDelta applied successfully!")
	return nil
}

func writeBlob(w OCIWriter, reader OCIReader, d digest.Digest) error {
	name := blobTarName(d)
	r, size, err := reader.ReadFile(name)
	if err != nil {
		return err
	}
	defer r.Close()
	return w.WriteFileFromReader(name, size, r)
}

func writeFileFromPath(w OCIWriter, name string, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	return w.WriteFileFromReader(name, info.Size(), f)
}

func processLayerDiff(tmpDir string, log Logger, writer OCIWriter, tarDiffReader io.Reader, expectedDiffID digest.Digest, dataSource tarpatch.DataSource) (newDigest digest.Digest, newSize int64, err error) {
	tmpFile, err := os.CreateTemp(tmpDir, "oci-delta-layer-*.gz")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	diffIDHash := sha256.New()
	compressedHash := sha256.New()

	// Create gzip writer that writes to both compressedHash and tmpFile
	compressedMulti := io.MultiWriter(compressedHash, tmpFile)
	gzWriter, err := gzip.NewWriterLevel(compressedMulti, gzip.DefaultCompression)
	if err != nil {
		return "", 0, err
	}
	gzWriter.Name = ""
	gzWriter.ModTime = time.Unix(0, 0)

	// Chain: tar-patch → [diffIDHash, gzWriter] → [compressedHash, tmpFile]
	uncompressedMulti := io.MultiWriter(diffIDHash, gzWriter)

	if err := tarpatch.Apply(tarDiffReader, dataSource, uncompressedMulti); err != nil {
		gzWriter.Close()
		return "", 0, fmt.Errorf("tar-patch failed: %w", err)
	}
	if err := gzWriter.Close(); err != nil {
		return "", 0, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	// Get the diff_id from the uncompressed hash
	actualDiffID := digest.NewDigestFromBytes(digest.SHA256, diffIDHash.Sum(nil))
	log.Debug("    Computed diff_id: %s", actualDiffID.Encoded()[:16])

	if expectedDiffID != "" {
		log.Debug("    Expected diff_id: %s", expectedDiffID.Encoded()[:16])
		if actualDiffID != expectedDiffID {
			return "", 0, fmt.Errorf("diff_id mismatch: expected %s, got %s",
				expectedDiffID.Encoded()[:16], actualDiffID.Encoded()[:16])
		}
		log.Debug("    Validated diff_id: %s", actualDiffID.Encoded()[:16])
	}

	// Get the compressed digest and size
	newDigest = digest.NewDigestFromBytes(digest.SHA256, compressedHash.Sum(nil))
	info, err := tmpFile.Stat()
	if err != nil {
		return "", 0, err
	}
	newSize = info.Size()
	log.Debug("    Compressed to %d bytes, new digest: %s", newSize, newDigest.Encoded()[:16])

	if _, err := tmpFile.Seek(0, 0); err != nil {
		return "", 0, err
	}
	if err := writer.WriteFileFromReader(blobTarName(newDigest), newSize, tmpFile); err != nil {
		return "", 0, fmt.Errorf("failed to write reconstructed layer: %w", err)
	}

	return newDigest, newSize, nil
}
