package bootcdelta

import (
	"archive/tar"
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
	Debug          func(format string, args ...interface{})
	Warning        func(format string, args ...interface{})
}

func getDataSource(opts *ApplyOptions, sourceConfigDigest string) (deltaDataSource, error) {
	if opts.DeltaSource != "" {
		return &simpleDataSource{tarpatch.NewFilesystemDataSource(opts.DeltaSource)}, nil
	}
	if opts.ContainerStore != nil {
		return resolveContainerStorageDataSource(opts.ContainerStore, sourceConfigDigest, opts.Debug)
	}
	ds, err := resolveOstreeDataSource(opts.RepoPath, sourceConfigDigest, opts.Debug)
	if err != nil {
		return nil, err
	}
	return &simpleDataSource{ds}, nil
}

func ApplyDelta(opts ApplyOptions) error {
	opts.Debug("Applying delta: %s", opts.DeltaPath)
	opts.Debug("Output: %s", opts.OutputPath)
	if opts.DeltaSource != "" {
		opts.Debug("Delta source: %s", opts.DeltaSource)
	} else if opts.ContainerStore != nil {
		opts.Debug("Container storage")
	} else {
		opts.Debug("Ostree repo: %s", opts.RepoPath)
	}

	opts.Debug("\nParsing delta...")
	delta, err := parseDeltaArtifact(opts.DeltaPath, opts.Debug, opts.Warning)
	if err != nil {
		return err
	}
	defer delta.Close()

	dataSource, err := getDataSource(&opts, delta.sourceConfigDigest)
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}
	defer func() {
		_ = dataSource.Close()
		_ = dataSource.Cleanup()
	}()

	// Reconstruct diff_id lookup from image config.
	layerDiffIDs := delta.imageConfig.RootFS.DiffIDs

	outFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	tarWriter := tar.NewWriter(outFile)
	defer tarWriter.Close()

	opts.Debug("\nWriting oci-layout")
	if err := writeTarFile(tarWriter, "oci-layout", ociLayoutFileData); err != nil {
		return err
	}

	// Write image config blob (unchanged).
	if err := delta.WriteBlobToTar(tarWriter, delta.imageConfigDigest); err != nil {
		return fmt.Errorf("failed to write image config: %w", err)
	}

	// Process each layer in the image manifest.
	// For reconstructed layers we need to remap the digest.
	outputLayers := make([]v1.Descriptor, len(delta.imageManifest.Layers))
	copy(outputLayers, delta.imageManifest.Layers)

	opts.Debug("\nProcessing layers...")
	for i, layer := range delta.imageManifest.Layers {
		deltaLayer, inDelta := delta.deltaLayerByTo[layer.Digest]
		if !inDelta {
			// Reused layer: keep original descriptor, no blob written.
			opts.Debug("  Layer %s: skipped (not in delta)", layer.Digest.Encoded()[:16])
			continue
		}

		var expectedDiffID digest.Digest
		if i < len(layerDiffIDs) {
			expectedDiffID = layerDiffIDs[i]
		}

		if deltaLayer.MediaType == mediaTypeTarDiff {
			opts.Debug("  Layer %s: reconstructing from tar-diff", layer.Digest.Encoded()[:16])
			r, err := delta.GetBlobReader(deltaLayer.Digest)
			if err != nil {
				return fmt.Errorf("failed to read tar-diff for layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
			newDigest, newSize, err := processLayerDiff(&opts, tarWriter, r, expectedDiffID, dataSource)
			if err != nil {
				return err
			}
			outputLayers[i].Digest = newDigest
			outputLayers[i].Size = newSize
		} else {
			opts.Debug("  Layer %s: copying original (%d bytes)", layer.Digest.Encoded()[:16], deltaLayer.Size)
			if err := delta.WriteBlobToTar(tarWriter, layer.Digest); err != nil {
				return fmt.Errorf("failed to copy layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
		}
	}

	// Build and write the output image manifest.
	opts.Debug("\nWriting output image manifest...")
	outputManifest := delta.imageManifest
	outputManifest.Layers = outputLayers
	outputManifestData, err := json.Marshal(outputManifest)
	if err != nil {
		return fmt.Errorf("failed to marshal output manifest: %w", err)
	}
	outputManifestDigest := computeDigest(outputManifestData)
	if err := writeTarFile(tarWriter, blobTarName(outputManifestDigest), outputManifestData); err != nil {
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
	opts.Debug("\nWriting index.json")
	if err := writeTarFile(tarWriter, "index.json", indexData); err != nil {
		return err
	}

	opts.Debug("\nDelta applied successfully!")
	return nil
}

func processLayerDiff(opts *ApplyOptions, tarWriter *tar.Writer, tarDiffReader io.Reader, expectedDiffID digest.Digest, dataSource tarpatch.DataSource) (newDigest digest.Digest, newSize int64, err error) {
	tmpFile, err := os.CreateTemp(opts.TmpDir, "bootc-delta-layer-*.gz")
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
	opts.Debug("    Computed diff_id: %s", actualDiffID.Encoded()[:16])

	if expectedDiffID != "" {
		opts.Debug("    Expected diff_id: %s", expectedDiffID.Encoded()[:16])
		if actualDiffID != expectedDiffID {
			return "", 0, fmt.Errorf("diff_id mismatch: expected %s, got %s",
				expectedDiffID.Encoded()[:16], actualDiffID.Encoded()[:16])
		}
		opts.Debug("    Validated diff_id: %s", actualDiffID.Encoded()[:16])
	}

	// Get the compressed digest and size
	newDigest = digest.NewDigestFromBytes(digest.SHA256, compressedHash.Sum(nil))
	info, err := tmpFile.Stat()
	if err != nil {
		return "", 0, err
	}
	newSize = info.Size()
	opts.Debug("    Compressed to %d bytes, new digest: %s", newSize, newDigest.Encoded()[:16])

	// Stream the compressed file into the tar archive
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return "", 0, err
	}
	if err := writeTarFileFromReader(tarWriter, blobTarName(newDigest), newSize, tmpFile); err != nil {
		return "", 0, fmt.Errorf("failed to write reconstructed layer: %w", err)
	}

	return newDigest, newSize, nil
}
