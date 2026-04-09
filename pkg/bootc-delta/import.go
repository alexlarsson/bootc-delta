package bootcdelta

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/containers/storage"
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ImportOptions struct {
	DeltaPath string
	Store     storage.Store
	Tag       string
	TmpDir    string
	Debug     func(format string, args ...interface{})
	Warning   func(format string, args ...interface{})
}

func ImportDelta(opts ImportOptions) (string, error) {
	opts.Debug("Importing delta: %s", opts.DeltaPath)

	opts.Debug("\nParsing delta artifact...")
	delta, err := parseDeltaArtifact(opts.DeltaPath, opts.Debug, opts.Warning)
	if err != nil {
		return "", err
	}
	defer delta.Close()

	dataSource, err := resolveContainerStorageDataSource(opts.Store, delta.sourceConfigDigest, opts.Debug)
	if err != nil {
		return "", fmt.Errorf("failed to create data source: %w", err)
	}
	defer func() {
		_ = dataSource.Close()
		_ = dataSource.Cleanup()
	}()

	layerDiffIDs := delta.imageConfig.RootFS.DiffIDs
	parentLayerID := ""
	storageLayers := make([]*storage.Layer, len(delta.imageManifest.Layers))

	opts.Debug("\nProcessing layers...")
	for i, layer := range delta.imageManifest.Layers {
		var diffID digest.Digest
		if i < len(layerDiffIDs) {
			diffID = layerDiffIDs[i]
		}

		deltaLayer, inDelta := delta.deltaLayerByTo[layer.Digest]
		if !inDelta {
			sl, err := reuseStorageLayer(opts.Store, diffID, parentLayerID, opts.TmpDir, opts.Debug)
			if err != nil {
				return "", err
			}
			storageLayers[i] = sl
			parentLayerID = sl.ID
		} else if deltaLayer.MediaType == mediaTypeTarDiff {
			opts.Debug("  Layer %d: reconstructing from tar-diff", i)
			r, err := delta.GetBlobReader(deltaLayer.Digest)
			if err != nil {
				return "", fmt.Errorf("failed to read tar-diff: %w", err)
			}

			pr, pw := io.Pipe()
			go func() {
				pw.CloseWithError(tarpatch.Apply(r, dataSource, pw))
			}()

			newLayer, _, err := opts.Store.PutLayer("", parentLayerID, nil, "", false, nil, pr)
			pr.Close()
			if err != nil {
				return "", fmt.Errorf("failed to store reconstructed layer: %w", err)
			}
			storageLayers[i] = newLayer
			parentLayerID = newLayer.ID
			opts.Debug("    Created layer %s", newLayer.ID[:16])
		} else {
			opts.Debug("  Layer %d: importing original layer", i)
			r, err := delta.GetBlobReader(layer.Digest)
			if err != nil {
				return "", fmt.Errorf("failed to read layer: %w", err)
			}
			gzReader, err := gzip.NewReader(r)
			if err != nil {
				return "", fmt.Errorf("failed to decompress layer: %w", err)
			}

			newLayer, _, err := opts.Store.PutLayer("", parentLayerID, nil, "", false, nil, gzReader)
			gzReader.Close()
			if err != nil {
				return "", fmt.Errorf("failed to store layer: %w", err)
			}
			storageLayers[i] = newLayer
			parentLayerID = newLayer.ID
			opts.Debug("    Created layer %s", newLayer.ID[:16])
		}
	}

	outputManifest := delta.imageManifest
	outputManifest.Layers = make([]v1.Descriptor, len(delta.imageManifest.Layers))
	copy(outputManifest.Layers, delta.imageManifest.Layers)
	for i, sl := range storageLayers {
		if sl.CompressedDigest != "" {
			outputManifest.Layers[i].Digest = sl.CompressedDigest
			outputManifest.Layers[i].Size = sl.CompressedSize
		} else if sl.UncompressedDigest != "" {
			outputManifest.Layers[i].Digest = sl.UncompressedDigest
			outputManifest.Layers[i].Size = sl.UncompressedSize
		}
	}

	manifestData, err := json.Marshal(outputManifest)
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}
	manifestDigest := digest.FromBytes(manifestData)

	var names []string
	if opts.Tag != "" {
		names = []string{opts.Tag}
	}
	image, err := opts.Store.CreateImage("", names, parentLayerID, "", nil)
	if err != nil {
		return "", fmt.Errorf("failed to create image: %w", err)
	}
	opts.Debug("\nCreated image %s", image.ID)

	if err := opts.Store.SetImageBigData(image.ID, "manifest", manifestData, manifestDigestFunc); err != nil {
		return "", fmt.Errorf("failed to store manifest: %w", err)
	}
	manifestKey := storage.ImageDigestManifestBigDataNamePrefix + "-" + manifestDigest.String()
	if err := opts.Store.SetImageBigData(image.ID, manifestKey, manifestData, manifestDigestFunc); err != nil {
		return "", fmt.Errorf("failed to store manifest: %w", err)
	}

	configData, err := delta.ReadBlob(delta.imageConfigDigest)
	if err != nil {
		return "", fmt.Errorf("failed to read config: %w", err)
	}
	if err := opts.Store.SetImageBigData(image.ID, delta.imageConfigDigest.String(), configData, nil); err != nil {
		return "", fmt.Errorf("failed to store config: %w", err)
	}

	opts.Debug("Import complete!")
	return image.ID, nil
}

func reuseStorageLayer(store storage.Store, diffID digest.Digest, parentLayerID string, tmpDir string, debug func(format string, args ...interface{})) (*storage.Layer, error) {
	debug("  Layer reused (diff_id %s)", diffID.Encoded()[:16])

	existing, err := store.LayersByUncompressedDigest(diffID)
	if err != nil {
		return nil, fmt.Errorf("failed to look up layer by diff_id %s: %w", diffID, err)
	}

	for i := range existing {
		if existing[i].Parent == parentLayerID {
			return &existing[i], nil
		}
	}

	if len(existing) > 0 {
		debug("    Recreating with correct parent chain")
		el := existing[0]
		diffReader, err := store.Diff(el.Parent, el.ID, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to extract layer diff: %w", err)
		}
		tmpFile, err := os.CreateTemp(tmpDir, "bootc-delta-layer-*.tar")
		if err != nil {
			diffReader.Close()
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()
		if _, err := io.Copy(tmpFile, diffReader); err != nil {
			diffReader.Close()
			return nil, fmt.Errorf("failed to buffer layer diff: %w", err)
		}
		diffReader.Close()
		if _, err := tmpFile.Seek(0, 0); err != nil {
			return nil, err
		}
		newLayer, _, err := store.PutLayer("", parentLayerID, nil, "", false, nil, tmpFile)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate layer: %w", err)
		}
		return newLayer, nil
	}

	return nil, fmt.Errorf("layer with diff_id %s not found in storage", diffID)
}

func manifestDigestFunc(data []byte) (digest.Digest, error) {
	return digest.FromBytes(data), nil
}
