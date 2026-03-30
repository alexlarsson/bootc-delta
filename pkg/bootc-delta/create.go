package bootcdelta

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	tardiff "github.com/containers/tar-diff/pkg/tar-diff"
	digest "github.com/opencontainers/go-digest"
)

type CreateOptions struct {
	OldImage    string
	NewImage    string
	OutputPath  string
	TmpDir      string
	Verbose     bool
	Parallelism int // max concurrent tar-diff workers; 0 means GOMAXPROCS
	Debug       func(format string, args ...interface{})
	Warning     func(format string, args ...interface{})
}

type CreateStats struct {
	OldLayers           int
	NewLayers           int
	ProcessedLayers     int
	SkippedLayers       int
	ProcessedLayerBytes int64
	TarDiffLayerBytes   int64
	OriginalLayerBytes  int64
}

func CreateDelta(opts CreateOptions) (*CreateStats, error) {
	stats := &CreateStats{}

	opts.Debug("Indexing old image: %s", opts.OldImage)
	oldTarIndex, err := indexTarArchive(opts.OldImage)
	if err != nil {
		return nil, fmt.Errorf("failed to index old image: %w", err)
	}
	defer oldTarIndex.Close()

	opts.Debug("Indexing new image: %s", opts.NewImage)
	newTarIndex, err := indexTarArchive(opts.NewImage)
	if err != nil {
		return nil, fmt.Errorf("failed to index new image: %w", err)
	}
	defer newTarIndex.Close()

	opts.Debug("Parsing old image")
	old, err := parseOCIImage(oldTarIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old image: %w", err)
	}
	stats.OldLayers = len(old.layers)
	opts.Debug("  Found %d layers in old image", stats.OldLayers)

	opts.Debug("Parsing new image")
	new, err := parseOCIImage(newTarIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new image: %w", err)
	}
	stats.NewLayers = len(new.layers)
	opts.Debug("  Found %d layers in new image", stats.NewLayers)

	// Find layers with new content (diff_id not in old image)
	newOnlyLayers := make(map[digest.Digest]bool)
	for diffID, newLayerDigest := range new.diffIDToDigest {
		if _, exists := old.diffIDToDigest[diffID]; !exists {
			newOnlyLayers[newLayerDigest] = true
			opts.Debug("  New layer: %s (diff_id: %s)", newLayerDigest.Encoded()[:16], diffID.Encoded()[:16])
		}
	}
	stats.ProcessedLayers = len(newOnlyLayers)
	stats.SkippedLayers = len(new.layers) - len(newOnlyLayers)
	opts.Debug("Layers with new content (will process): %d", stats.ProcessedLayers)
	opts.Debug("Layers with existing content (will skip): %d", stats.SkippedLayers)

	outFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	tarWriter := tar.NewWriter(outFile)
	defer tarWriter.Close()

	opts.Debug("\nWriting oci-layout")
	ociLayoutData, err := new.tarIndex.ReadFile("oci-layout")
	if err != nil {
		return nil, fmt.Errorf("failed to read oci-layout: %w", err)
	}
	if err := writeTarFile(tarWriter, "oci-layout", ociLayoutData); err != nil {
		return nil, err
	}

	opts.Debug("\nWriting index.json")
	indexData, err := json.Marshal(new.index)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index: %w", err)
	}
	if err := writeTarFile(tarWriter, "index.json", indexData); err != nil {
		return nil, err
	}

	opts.Debug("\nProcessing layers...")
	for layerDigest := range new.layers {
		if !newOnlyLayers[layerDigest] {
			opts.Debug("  Skipping layer with existing content %s", layerDigest.Encoded()[:16])
		}
	}
	layerResults, err := computeLayerDiffsParallel(&opts, old, new, newOnlyLayers, opts.TmpDir)
	if err != nil {
		return nil, err
	}
	for _, r := range layerResults {
		defer os.Remove(r.diffPath)
		stats.ProcessedLayerBytes += r.originalSize
		if r.diffPath != "" {
			opts.Debug("  Layer %s: using tar-diff (%d bytes, saved %d)", r.digest.Encoded()[:16], r.diffSize, r.originalSize-r.diffSize)
			if err := writeTarFileFromFile(tarWriter, blobTarName(r.digest), r.diffPath); err != nil {
				return nil, err
			}
			stats.TarDiffLayerBytes += r.diffSize
		} else {
			opts.Debug("  Layer %s: using original (%d bytes)", r.digest.Encoded()[:16], r.originalSize)
			stats.OriginalLayerBytes += r.originalSize
			if err := writeBlobTarFile(tarWriter, new.tarIndex, r.digest); err != nil {
				return nil, err
			}
		}
	}

	// We need to also copy the remaining blobs, like the manifest and the config json
	opts.Debug("\nWriting non-layer blobs...")
	for name := range new.tarIndex.entries {
		if !isBlobPath(name) {
			continue
		}
		d := digestFromBlobPath(name)
		if d != "" && !new.layers[d] {
			if err := writeBlobTarFile(tarWriter, new.tarIndex, d); err != nil {
				return nil, err
			}
		}
	}

	return stats, nil
}

type layerDiffResult struct {
	digest       digest.Digest
	originalSize int64
	diffPath     string // temp file path; empty means use original layer
	diffSize     int64
}

func computeLayerDiffsParallel(opts *CreateOptions, old *OCIImage, new *OCIImage, newOnlyLayers map[digest.Digest]bool, tmpDir string) ([]layerDiffResult, error) {
	layers := make([]digest.Digest, 0, len(newOnlyLayers))
	for d := range newOnlyLayers {
		layers = append(layers, d)
	}

	results := make([]layerDiffResult, len(layers))
	errs := make([]error, len(layers))

	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	total := len(layers)
	for i, d := range layers {
		i, d := i, d
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i], errs[i] = computeLayerDiff(opts, old, new, d, i+1, total, tmpDir)
		}()
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			for _, r := range results {
				if r.diffPath != "" {
					os.Remove(r.diffPath)
				}
			}
			return nil, err
		}
	}

	return results, nil
}

func computeLayerDiff(opts *CreateOptions, old *OCIImage, new *OCIImage, blobDigest digest.Digest, layerNum, total int, tmpDir string) (layerDiffResult, error) {
	originalSize, err := new.tarIndex.GetSize(blobTarName(blobDigest))
	if err != nil {
		return layerDiffResult{}, fmt.Errorf("failed to get layer size %s: %w", blobDigest.Encoded()[:16], err)
	}

	opts.Debug("  Computing diff for layer %d/%d %s (%d bytes)", layerNum, total, blobDigest.Encoded()[:16], originalSize)

	tmpFile, err := os.CreateTemp(tmpDir, "bootc-delta-*.tar-diff")
	if err != nil {
		return layerDiffResult{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	diffPath := tmpFile.Name()
	tmpFile.Close()

	if err := runTarDiff(old, new, blobDigest, diffPath); err != nil {
		opts.Warning("tar-diff failed for layer %s: %v, using original", blobDigest.Encoded()[:16], err)
		os.Remove(diffPath)
		return layerDiffResult{digest: blobDigest, originalSize: originalSize}, nil
	}

	info, err := os.Stat(diffPath)
	if err != nil || info.Size() >= originalSize {
		os.Remove(diffPath)
		return layerDiffResult{digest: blobDigest, originalSize: originalSize}, nil
	}

	return layerDiffResult{digest: blobDigest, originalSize: originalSize, diffPath: diffPath, diffSize: info.Size()}, nil
}

func runTarDiff(old *OCIImage, new *OCIImage, newLayerDigest digest.Digest, output string) error {
	var oldFiles []io.ReadSeeker

	// Get readers for all old image layers
	for layerDigest := range old.layers {
		r, err := old.tarIndex.GetReader(blobTarName(layerDigest))
		if err != nil {
			return err
		}
		oldFiles = append(oldFiles, r)
	}

	// Get reader for new layer
	newFile, err := new.tarIndex.GetReader(blobTarName(newLayerDigest))
	if err != nil {
		return err
	}

	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()

	opts := tardiff.NewOptions()
	opts.SetSourcePrefixes([]string{"sysroot/ostree/repo/objects/"})

	return tardiff.Diff(oldFiles, newFile, outFile, opts)
}
