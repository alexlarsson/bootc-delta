package ocidelta

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
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type CreateOptions struct {
	OldImage    string
	NewImage    string
	OutputPath  string
	TmpDir      string
	Verbose     bool
	Parallelism int // max concurrent tar-diff workers; 0 means GOMAXPROCS
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

func CreateDelta(opts CreateOptions, log Logger) (*CreateStats, error) {
	stats := &CreateStats{}

	log.Debug("Opening old image: %s", opts.OldImage)
	oldStore, err := OpenBlobStore(opts.OldImage)
	if err != nil {
		return nil, fmt.Errorf("failed to open old image: %w", err)
	}
	defer oldStore.Close()

	log.Debug("Opening new image: %s", opts.NewImage)
	newStore, err := OpenBlobStore(opts.NewImage)
	if err != nil {
		return nil, fmt.Errorf("failed to open new image: %w", err)
	}
	defer newStore.Close()

	log.Debug("Parsing old image")
	old, err := parseOCIImage(oldStore)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old image: %w", err)
	}
	stats.OldLayers = len(old.layers)
	log.Debug("  Found %d layers in old image", stats.OldLayers)

	log.Debug("Parsing new image")
	new, err := parseOCIImage(newStore)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new image: %w", err)
	}
	stats.NewLayers = len(new.layers)
	log.Debug("  Found %d layers in new image", stats.NewLayers)

	// Find layers with new content (diff_id not in old image)
	newOnlyLayers := make(map[digest.Digest]bool)
	oldReusedLayers := make(map[digest.Digest]bool)
	for _, newLayer := range new.layers {
		if oldLayer, exists := old.layerByDiffID[newLayer.DiffID]; exists {
			oldReusedLayers[oldLayer.Digest] = true
		} else {
			newOnlyLayers[newLayer.Digest] = true
			log.Debug("  New layer: %s (diff_id: %s)", newLayer.Digest.Encoded()[:16], newLayer.DiffID.Encoded()[:16])
		}
	}
	stats.ProcessedLayers = len(newOnlyLayers)
	stats.SkippedLayers = len(new.layers) - len(newOnlyLayers)
	log.Debug("Layers with new content (will process): %d", stats.ProcessedLayers)
	log.Debug("Layers with existing content (will skip): %d", stats.SkippedLayers)

	log.Debug("\nProcessing layers...")
	for _, l := range new.layers {
		if !newOnlyLayers[l.Digest] {
			log.Debug("  Skipping layer with existing content %s", l.Digest.Encoded()[:16])
		}
	}
	layerResults, err := computeLayerDiffsParallel(log, old, new, newOnlyLayers, opts.TmpDir, opts.Parallelism)
	if err != nil {
		return nil, err
	}
	for _, r := range layerResults {
		defer os.Remove(r.diffPath)
	}

	// Build a map from new layer digest to result for ordered iteration.
	layerResultByDigest := make(map[digest.Digest]layerDiffResult)
	for _, r := range layerResults {
		layerResultByDigest[r.digest] = r
	}

	// Read embedded image manifest and config data.
	imageManifestDesc := new.index.Manifests[0]
	imageManifestData, err := readAllFromStore(new.blobStore, blobTarName(imageManifestDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read new image manifest: %w", err)
	}
	imageConfigData, err := readAllFromStore(new.blobStore, blobTarName(new.manifest.Config.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read new image config: %w", err)
	}

	// Build delta manifest layers (image manifest + config first, then layer blobs).
	var deltaLayers []v1.Descriptor
	deltaLayers = append(deltaLayers, v1.Descriptor{
		MediaType: v1.MediaTypeImageManifest,
		Digest:    imageManifestDesc.Digest,
		Size:      int64(len(imageManifestData)),
	})
	deltaLayers = append(deltaLayers, v1.Descriptor{
		MediaType: v1.MediaTypeImageConfig,
		Digest:    new.manifest.Config.Digest,
		Size:      int64(len(imageConfigData)),
	})

	var reusedDigests, reusedDiffIDs []string
	for _, l := range new.layers {
		if !newOnlyLayers[l.Digest] {
			// Collect old reused non-delta layers
			reusedDigests = append(reusedDigests, l.Digest.String())
			reusedDiffIDs = append(reusedDiffIDs, l.DiffID.String())
			continue
		}
		r := layerResultByDigest[l.Digest]
		stats.ProcessedLayerBytes += r.originalSize

		annotations := map[string]string{
			annotationDeltaTo: l.Digest.String(),
		}
		var desc v1.Descriptor
		if r.diffPath != "" {
			log.Debug("  Layer %s: using tar-diff (%d bytes, saved %d)", r.digest.Encoded()[:16], r.diffSize, r.originalSize-r.diffSize)
			desc = v1.Descriptor{
				MediaType:   mediaTypeTarDiff,
				Digest:      r.diffDigest,
				Size:        r.diffSize,
				Annotations: annotations,
			}
			stats.TarDiffLayerBytes += r.diffSize
		} else {
			log.Debug("  Layer %s: using original (%d bytes)", r.digest.Encoded()[:16], r.originalSize)
			desc = v1.Descriptor{
				MediaType:   v1.MediaTypeImageLayerGzip,
				Digest:      r.digest,
				Size:        r.originalSize,
				Annotations: annotations,
			}
			stats.OriginalLayerBytes += r.originalSize
		}
		deltaLayers = append(deltaLayers, desc)
	}

	// Build delta manifest.
	deltaConfigData := []byte("{}")
	deltaConfigDigest := computeDigest(deltaConfigData)
	deltaAnnotations := map[string]string{
		annotationDeltaTarget:       imageManifestDesc.Digest.String(),
		annotationDeltaSource:       old.manifestDigest.String(),
		annotationDeltaSourceConfig: old.configDigest.String(),
	}
	if len(reusedDigests) > 0 {
		reusedJSON, _ := json.Marshal(reusedDigests)
		deltaAnnotations[annotationDeltaReused] = string(reusedJSON)
		reusedDiffIDJSON, _ := json.Marshal(reusedDiffIDs)
		deltaAnnotations[annotationDeltaReusedDiffID] = string(reusedDiffIDJSON)
	}
	deltaManifest := v1.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Config: v1.Descriptor{
			MediaType: mediaTypeDeltaConfig,
			Digest:    deltaConfigDigest,
			Size:      int64(len(deltaConfigData)),
		},
		Annotations: deltaAnnotations,
		Layers:      deltaLayers,
	}
	deltaManifestData, err := json.Marshal(deltaManifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal delta manifest: %w", err)
	}
	deltaManifestDigest := computeDigest(deltaManifestData)

	// Build OCI index pointing to the delta manifest.
	ociIndex := v1.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: v1.MediaTypeImageIndex,
		Manifests: []v1.Descriptor{{
			MediaType: v1.MediaTypeImageManifest,
			Digest:    deltaManifestDigest,
			Size:      int64(len(deltaManifestData)),
		}},
	}
	indexData, err := json.Marshal(ociIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index: %w", err)
	}

	// Write output archive.
	outFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	tarWriter := tar.NewWriter(outFile)
	defer tarWriter.Close()

	log.Debug("\nWriting oci-layout")
	if err := writeTarFile(tarWriter, "oci-layout", ociLayoutFileData); err != nil {
		return nil, err
	}
	if err := writeTarDir(tarWriter, "blobs/"); err != nil {
		return nil, err
	}
	if err := writeTarDir(tarWriter, "blobs/sha256/"); err != nil {
		return nil, err
	}

	log.Debug("Writing image manifest and config blobs")
	if err := writeTarFile(tarWriter, blobTarName(imageManifestDesc.Digest), imageManifestData); err != nil {
		return nil, err
	}
	if err := writeTarFile(tarWriter, blobTarName(new.manifest.Config.Digest), imageConfigData); err != nil {
		return nil, err
	}

	log.Debug("Writing layer blobs")
	for _, l := range new.layers {
		if !newOnlyLayers[l.Digest] {
			continue
		}
		r := layerResultByDigest[l.Digest]
		if r.diffPath != "" {
			if err := writeTarFileFromFile(tarWriter, blobTarName(r.diffDigest), r.diffPath); err != nil {
				return nil, err
			}
		} else {
			if err := writeBlobTarFile(tarWriter, new.blobStore, r.digest); err != nil {
				return nil, err
			}
		}
	}

	log.Debug("Writing delta manifest and index.json")
	if err := writeTarFile(tarWriter, blobTarName(deltaConfigDigest), deltaConfigData); err != nil {
		return nil, err
	}
	if err := writeTarFile(tarWriter, blobTarName(deltaManifestDigest), deltaManifestData); err != nil {
		return nil, err
	}
	if err := writeTarFile(tarWriter, "index.json", indexData); err != nil {
		return nil, err
	}

	return stats, nil
}

type layerDiffResult struct {
	digest       digest.Digest
	originalSize int64
	diffPath     string // temp file path; empty means use original layer
	diffSize     int64
	diffDigest   digest.Digest // sha256 of the diff file blob
}

func computeLayerDiffsParallel(log Logger, old *OCIImage, new *OCIImage, newOnlyLayers map[digest.Digest]bool, tmpDir string, parallelism int) ([]layerDiffResult, error) {
	layers := make([]digest.Digest, 0, len(newOnlyLayers))
	for d := range newOnlyLayers {
		layers = append(layers, d)
	}

	// Pre-analyze old layers once (shared across all diffs)
	log.Debug("  Analyzing source layers...")
	diffOpts := tardiff.NewOptions()
	diffOpts.SetIgnoreSourcePrefixes([]string{"sysroot/ostree/"})
	diffOpts.SetApplyWhiteouts(true)
	diffOpts.SetTmpDir(tmpDir)

	var oldFiles []io.ReadSeeker
	for _, layer := range old.layers {
		r, _, err := old.blobStore.ReadFile(blobTarName(layer.Digest))
		if err != nil {
			return nil, fmt.Errorf("failed to get old layer reader: %w", err)
		}
		defer r.Close()
		oldFiles = append(oldFiles, r)
	}

	sources, err := tardiff.AnalyzeSources(oldFiles, diffOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze sources: %w", err)
	}

	results := make([]layerDiffResult, len(layers))
	errs := make([]error, len(layers))

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
			results[i], errs[i] = computeLayerDiff(log, old, new, d, i+1, total, tmpDir, sources, diffOpts)
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

func computeLayerDiff(log Logger, old *OCIImage, new *OCIImage, blobDigest digest.Digest, layerNum, total int, tmpDir string, sources *tardiff.SourceAnalysis, diffOpts *tardiff.Options) (layerDiffResult, error) {
	sizeReader, originalSize, err := new.blobStore.ReadFile(blobTarName(blobDigest))
	if err != nil {
		return layerDiffResult{}, fmt.Errorf("failed to get layer size %s: %w", blobDigest.Encoded()[:16], err)
	}
	sizeReader.Close()

	log.Debug("  Computing diff for layer %d/%d %s (%d bytes)", layerNum, total, blobDigest.Encoded()[:16], originalSize)

	tmpFile, err := os.CreateTemp(tmpDir, "oci-delta-*.tar-diff")
	if err != nil {
		return layerDiffResult{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	diffPath := tmpFile.Name()
	tmpFile.Close()

	if err := runTarDiff(old, new, blobDigest, diffPath, sources, diffOpts); err != nil {
		log.Warning("tar-diff failed for layer %s: %v, using original", blobDigest.Encoded()[:16], err)
		os.Remove(diffPath)
		return layerDiffResult{digest: blobDigest, originalSize: originalSize}, nil
	}

	info, err := os.Stat(diffPath)
	if err != nil || info.Size() >= originalSize {
		os.Remove(diffPath)
		return layerDiffResult{digest: blobDigest, originalSize: originalSize}, nil
	}

	diffDigest, err := computeFileDigest(diffPath)
	if err != nil {
		os.Remove(diffPath)
		return layerDiffResult{}, fmt.Errorf("failed to compute diff digest: %w", err)
	}

	return layerDiffResult{digest: blobDigest, originalSize: originalSize, diffPath: diffPath, diffSize: info.Size(), diffDigest: diffDigest}, nil
}

func runTarDiff(old *OCIImage, new *OCIImage, newLayerDigest digest.Digest, output string, sources *tardiff.SourceAnalysis, diffOpts *tardiff.Options) error {
	var oldFiles []io.ReadSeeker

	for _, layer := range old.layers {
		r, _, err := old.blobStore.ReadFile(blobTarName(layer.Digest))
		if err != nil {
			return err
		}
		defer r.Close()
		oldFiles = append(oldFiles, r)
	}

	newFile, _, err := new.blobStore.ReadFile(blobTarName(newLayerDigest))
	if err != nil {
		return err
	}
	defer newFile.Close()

	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()

	return tardiff.DiffWithSources(sources, oldFiles, newFile, outFile, diffOpts)
}
