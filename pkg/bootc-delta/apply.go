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

	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ApplyOptions struct {
	DeltaPath   string
	OutputPath  string
	DeltaSource string
	TmpDir      string
	Debug       func(format string, args ...interface{})
	Warning     func(format string, args ...interface{})
}

// deltaArtifact holds the parsed contents of a delta OCI artifact.
type deltaArtifact struct {
	imageManifest       v1.Manifest
	imageConfig         v1.Image
	imageManifestDigest digest.Digest
	imageConfigDigest   digest.Digest
	// deltaLayerByTo maps delta.to digest → delta manifest layer descriptor
	deltaLayerByTo map[digest.Digest]v1.Descriptor
}

func parseDeltaArtifact(opts *ApplyOptions, tarIndex *TarIndex) (*deltaArtifact, error) {
	indexData, err := tarIndex.ReadFile("index.json")
	if err != nil {
		return nil, fmt.Errorf("delta archive does not contain index.json")
	}
	var ociIndex v1.Index
	if err := json.Unmarshal(indexData, &ociIndex); err != nil {
		return nil, fmt.Errorf("failed to parse index.json: %w", err)
	}
	if len(ociIndex.Manifests) == 0 {
		return nil, fmt.Errorf("delta archive contains no manifests")
	}

	deltaManifestDigest := ociIndex.Manifests[0].Digest
	opts.Debug("  Delta manifest: %s", deltaManifestDigest.Encoded()[:16])

	deltaManifestData, err := tarIndex.ReadFile(blobTarName(deltaManifestDigest))
	if err != nil {
		return nil, fmt.Errorf("failed to read delta manifest: %w", err)
	}
	var deltaManifest v1.Manifest
	if err := json.Unmarshal(deltaManifestData, &deltaManifest); err != nil {
		return nil, fmt.Errorf("failed to parse delta manifest: %w", err)
	}
	if deltaManifest.Config.MediaType != mediaTypeDeltaConfig {
		return nil, fmt.Errorf("not a delta artifact (config mediaType: %s)", deltaManifest.Config.MediaType)
	}
	// Find embedded image manifest and config by media type, and collect
	// delta layer descriptors (keyed by delta.to) from the remaining layers.
	var imageManifestDesc, imageConfigDesc *v1.Descriptor
	deltaLayerByTo := make(map[digest.Digest]v1.Descriptor)
	for i := range deltaManifest.Layers {
		layer := &deltaManifest.Layers[i]
		switch layer.MediaType {
		case v1.MediaTypeImageManifest:
			imageManifestDesc = layer
		case v1.MediaTypeImageConfig:
			imageConfigDesc = layer
		case mediaTypeTarDiff, v1.MediaTypeImageLayerGzip:
			toStr := layer.Annotations[annotationDeltaTo]
			if toStr == "" {
				continue
			}
			toDigest, err := digest.Parse(toStr)
			if err != nil {
				opts.Warning("invalid delta.to annotation %q: %v", toStr, err)
				continue
			}
			deltaLayerByTo[toDigest] = *layer
		}
	}
	if imageManifestDesc == nil {
		return nil, fmt.Errorf("delta manifest contains no embedded image manifest layer")
	}
	if imageConfigDesc == nil {
		return nil, fmt.Errorf("delta manifest contains no embedded image config layer")
	}

	imageManifestData, err := tarIndex.ReadFile(blobTarName(imageManifestDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded image manifest: %w", err)
	}
	var imageManifest v1.Manifest
	if err := json.Unmarshal(imageManifestData, &imageManifest); err != nil {
		return nil, fmt.Errorf("failed to parse embedded image manifest: %w", err)
	}
	opts.Debug("  Image manifest: %s (%d layers)", imageManifestDesc.Digest.Encoded()[:16], len(imageManifest.Layers))

	imageConfigData, err := tarIndex.ReadFile(blobTarName(imageConfigDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded image config: %w", err)
	}
	var imageConfig v1.Image
	if err := json.Unmarshal(imageConfigData, &imageConfig); err != nil {
		return nil, fmt.Errorf("failed to parse embedded image config: %w", err)
	}
	opts.Debug("  Image config: %s (%d diff_ids)", imageConfigDesc.Digest.Encoded()[:16], len(imageConfig.RootFS.DiffIDs))

	return &deltaArtifact{
		imageManifest:       imageManifest,
		imageConfig:         imageConfig,
		imageManifestDigest: imageManifestDesc.Digest,
		imageConfigDigest:   imageConfigDesc.Digest,
		deltaLayerByTo:      deltaLayerByTo,
	}, nil
}

func ApplyDelta(opts ApplyOptions) error {
	opts.Debug("Applying delta: %s", opts.DeltaPath)
	opts.Debug("Output: %s", opts.OutputPath)
	opts.Debug("Delta source: %s", opts.DeltaSource)

	opts.Debug("\nIndexing delta file...")
	deltaTarIndex, err := indexTarArchive(opts.DeltaPath)
	if err != nil {
		return fmt.Errorf("failed to index delta file: %w", err)
	}
	defer deltaTarIndex.Close()

	opts.Debug("\nParsing delta artifact...")
	artifact, err := parseDeltaArtifact(&opts, deltaTarIndex)
	if err != nil {
		return err
	}

	// Reconstruct diff_id lookup from image config.
	layerDiffIDs := artifact.imageConfig.RootFS.DiffIDs

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
	if err := writeBlobTarFile(tarWriter, deltaTarIndex, artifact.imageConfigDigest); err != nil {
		return fmt.Errorf("failed to write image config: %w", err)
	}

	// Process each layer in the image manifest.
	// For reconstructed layers we need to remap the digest.
	outputLayers := make([]v1.Descriptor, len(artifact.imageManifest.Layers))
	copy(outputLayers, artifact.imageManifest.Layers)

	opts.Debug("\nProcessing layers...")
	for i, layer := range artifact.imageManifest.Layers {
		deltaLayer, inDelta := artifact.deltaLayerByTo[layer.Digest]
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
			r, err := deltaTarIndex.GetReader(blobTarName(deltaLayer.Digest))
			if err != nil {
				return fmt.Errorf("failed to read tar-diff for layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
			newDigest, newSize, err := processLayerDiff(&opts, tarWriter, r, expectedDiffID)
			if err != nil {
				return err
			}
			outputLayers[i].Digest = newDigest
			outputLayers[i].Size = newSize
		} else {
			opts.Debug("  Layer %s: copying original (%d bytes)", layer.Digest.Encoded()[:16], deltaLayer.Size)
			if err := writeBlobTarFile(tarWriter, deltaTarIndex, layer.Digest); err != nil {
				return fmt.Errorf("failed to copy layer %s: %w", layer.Digest.Encoded()[:16], err)
			}
		}
	}

	// Build and write the output image manifest.
	opts.Debug("\nWriting output image manifest...")
	outputManifest := artifact.imageManifest
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

func processLayerDiff(opts *ApplyOptions, tarWriter *tar.Writer, tarDiffReader io.Reader, expectedDiffID digest.Digest) (newDigest digest.Digest, newSize int64, err error) {
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

	dataSource := tarpatch.NewFilesystemDataSource(opts.DeltaSource)
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
