package ocidelta

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	mediaTypeDeltaConfig        = "application/vnd.redhat.oci-delta.config.v1+json"
	mediaTypeTarDiff            = "application/vnd.tar-diff"
	annotationDeltaTarget       = "io.github.containers.delta.target"
	annotationDeltaSource       = "io.github.containers.delta.source"
	annotationDeltaSourceConfig = "io.github.containers.delta.source-config"
	annotationDeltaTo           = "io.github.containers.delta.to"
	annotationDeltaReused       = "io.github.containers.delta.reused"
	annotationDeltaReusedDiffID = "io.github.containers.delta.reused-diff-id"
)

// deltaArtifact holds the parsed contents of a delta OCI artifact.
type deltaArtifact struct {
	tarIndex            *TarIndex
	imageManifest       v1.Manifest
	imageConfig         v1.Image
	imageManifestDigest digest.Digest
	imageConfigDigest   digest.Digest
	sourceConfigDigest  string
	deltaLayerByTo      map[digest.Digest]v1.Descriptor
}

func parseDeltaArtifact(path string, log Logger) (*deltaArtifact, error) {
	tarIndex, err := indexTarArchive(path)
	if err != nil {
		return nil, fmt.Errorf("failed to index delta file: %w", err)
	}

	indexData, err := readAllFromStore(tarIndex, "index.json")
	if err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("delta archive does not contain index.json")
	}
	var ociIndex v1.Index
	if err := json.Unmarshal(indexData, &ociIndex); err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to parse index.json: %w", err)
	}
	if len(ociIndex.Manifests) == 0 {
		tarIndex.Close()
		return nil, fmt.Errorf("delta archive contains no manifests")
	}

	deltaManifestDigest := ociIndex.Manifests[0].Digest
	log.Debug("  Delta manifest: %s", deltaManifestDigest.Encoded()[:16])

	deltaManifestData, err := readAllFromStore(tarIndex, blobTarName(deltaManifestDigest))
	if err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to read delta manifest: %w", err)
	}
	var deltaManifest v1.Manifest
	if err := json.Unmarshal(deltaManifestData, &deltaManifest); err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to parse delta manifest: %w", err)
	}
	if deltaManifest.Config.MediaType != mediaTypeDeltaConfig {
		tarIndex.Close()
		return nil, fmt.Errorf("not a delta artifact (config mediaType: %s)", deltaManifest.Config.MediaType)
	}

	sourceConfigDigest := deltaManifest.Annotations[annotationDeltaSourceConfig]

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
				log.Warning("invalid delta.to annotation %q: %v", toStr, err)
				continue
			}
			deltaLayerByTo[toDigest] = *layer
		}
	}
	if imageManifestDesc == nil {
		tarIndex.Close()
		return nil, fmt.Errorf("delta manifest contains no embedded image manifest layer")
	}
	if imageConfigDesc == nil {
		tarIndex.Close()
		return nil, fmt.Errorf("delta manifest contains no embedded image config layer")
	}

	imageManifestData, err := readAllFromStore(tarIndex, blobTarName(imageManifestDesc.Digest))
	if err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to read embedded image manifest: %w", err)
	}
	var imageManifest v1.Manifest
	if err := json.Unmarshal(imageManifestData, &imageManifest); err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to parse embedded image manifest: %w", err)
	}
	log.Debug("  Image manifest: %s (%d layers)", imageManifestDesc.Digest.Encoded()[:16], len(imageManifest.Layers))

	imageConfigData, err := readAllFromStore(tarIndex, blobTarName(imageConfigDesc.Digest))
	if err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to read embedded image config: %w", err)
	}
	var imageConfig v1.Image
	if err := json.Unmarshal(imageConfigData, &imageConfig); err != nil {
		tarIndex.Close()
		return nil, fmt.Errorf("failed to parse embedded image config: %w", err)
	}
	log.Debug("  Image config: %s (%d diff_ids)", imageConfigDesc.Digest.Encoded()[:16], len(imageConfig.RootFS.DiffIDs))

	return &deltaArtifact{
		tarIndex:            tarIndex,
		imageManifest:       imageManifest,
		imageConfig:         imageConfig,
		imageManifestDigest: imageManifestDesc.Digest,
		imageConfigDigest:   imageConfigDesc.Digest,
		sourceConfigDigest:  sourceConfigDigest,
		deltaLayerByTo:      deltaLayerByTo,
	}, nil
}

func (d *deltaArtifact) Close() error {
	return d.tarIndex.Close()
}

func (d *deltaArtifact) ReadBlob(dgst digest.Digest) ([]byte, error) {
	return readAllFromStore(d.tarIndex, blobTarName(dgst))
}

func (d *deltaArtifact) GetBlobReader(dgst digest.Digest) (io.ReadSeekCloser, error) {
	r, _, err := d.tarIndex.ReadFile(blobTarName(dgst))
	return r, err
}

func (d *deltaArtifact) GetBlobSize(dgst digest.Digest) (int64, error) {
	r, size, err := d.tarIndex.ReadFile(blobTarName(dgst))
	if err != nil {
		return 0, err
	}
	r.Close()
	return size, nil
}

func (d *deltaArtifact) WriteBlobToTar(w *tar.Writer, dgst digest.Digest) error {
	return writeBlobTarFile(w, d.tarIndex, dgst)
}
