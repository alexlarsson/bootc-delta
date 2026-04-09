package bootcdelta

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Logger interface {
	Debug(format string, args ...interface{})
	Warning(format string, args ...interface{})
}

var ociLayoutFileData = []byte(`{"imageLayoutVersion":"1.0.0"}`)

type TarIndex struct {
	file    *os.File
	entries map[string]*TarEntry
}

type TarEntry struct {
	offset int64
	size   int64
}

type OCILayer struct {
	Digest digest.Digest
	DiffID digest.Digest
}

type OCIImage struct {
	index          *v1.Index
	manifest       *v1.Manifest
	manifestDigest digest.Digest
	configDigest   digest.Digest
	layers         []OCILayer
	layerByDigest  map[digest.Digest]*OCILayer
	layerByDiffID  map[digest.Digest]*OCILayer
	tarIndex       *TarIndex
}

type offsetTracker struct {
	r      io.Reader
	offset int64
}

func (ot *offsetTracker) Read(p []byte) (n int, err error) {
	n, err = ot.r.Read(p)
	ot.offset += int64(n)
	return
}

func indexTarArchive(path string) (*TarIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	tracker := &offsetTracker{r: f}
	tr := tar.NewReader(tracker)
	entries := make(map[string]*TarEntry)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return nil, err
		}

		// Current offset is where the data starts
		dataOffset := tracker.offset

		// Record entry
		entries[hdr.Name] = &TarEntry{
			offset: dataOffset,
			size:   hdr.Size,
		}

		// Skip the data
		if _, err := io.Copy(io.Discard, tr); err != nil {
			f.Close()
			return nil, err
		}
	}

	return &TarIndex{
		file:    f,
		entries: entries,
	}, nil
}

func (idx *TarIndex) ReadFile(name string) ([]byte, error) {
	entry, ok := idx.entries[name]
	if !ok {
		return nil, fmt.Errorf("file not found in tar: %s", name)
	}

	data := make([]byte, entry.size)
	_, err := idx.file.ReadAt(data, entry.offset)
	return data, err
}

func (idx *TarIndex) GetReader(name string) (io.ReadSeeker, error) {
	entry, ok := idx.entries[name]
	if !ok {
		return nil, fmt.Errorf("file not found in tar: %s", name)
	}

	// SectionReader uses ReadAt which is thread-safe
	return io.NewSectionReader(idx.file, entry.offset, entry.size), nil
}

func (idx *TarIndex) GetSize(name string) (int64, error) {
	entry, ok := idx.entries[name]
	if !ok {
		return 0, fmt.Errorf("file not found in tar: %s", name)
	}
	return entry.size, nil
}

func (idx *TarIndex) Close() error {
	if idx.file != nil {
		return idx.file.Close()
	}
	return nil
}

func parseOCIImage(tarIndex *TarIndex) (*OCIImage, error) {
	indexData, err := tarIndex.ReadFile("index.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read index.json: %w", err)
	}

	var index v1.Index
	if err := json.Unmarshal(indexData, &index); err != nil {
		return nil, fmt.Errorf("failed to parse index.json: %w", err)
	}

	if len(index.Manifests) == 0 {
		return nil, fmt.Errorf("OCI archive contains no manifests")
	}
	manifestDesc := index.Manifests[0]
	if manifestDesc.MediaType == "application/vnd.oci.image.index.v1+json" ||
		manifestDesc.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
		return nil, fmt.Errorf("OCI archive contains a manifest list, only single-image archives are supported")
	}
	if len(index.Manifests) > 1 {
		return nil, fmt.Errorf("OCI archive contains multiple manifests (%d), only single-image archives are supported", len(index.Manifests))
	}

	// Read the manifest
	manifestData, err := tarIndex.ReadFile(blobTarName(manifestDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest v1.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Read config to get diff_ids.
	if manifest.Config.Digest == "" {
		return nil, fmt.Errorf("manifest has no config digest")
	}

	configData, err := tarIndex.ReadFile(blobTarName(manifest.Config.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config v1.Image
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	layers := make([]OCILayer, len(manifest.Layers))
	layerByDigest := make(map[digest.Digest]*OCILayer, len(manifest.Layers))
	layerByDiffID := make(map[digest.Digest]*OCILayer, len(manifest.Layers))
	for i, l := range manifest.Layers {
		layers[i].Digest = l.Digest
		if i < len(config.RootFS.DiffIDs) {
			layers[i].DiffID = config.RootFS.DiffIDs[i]
		}
		layerByDigest[layers[i].Digest] = &layers[i]
		if layers[i].DiffID != "" {
			layerByDiffID[layers[i].DiffID] = &layers[i]
		}
	}

	return &OCIImage{
		index:          &index,
		manifest:       &manifest,
		manifestDigest: manifestDesc.Digest,
		configDigest:   manifest.Config.Digest,
		layers:         layers,
		layerByDigest:  layerByDigest,
		layerByDiffID:  layerByDiffID,
		tarIndex:       tarIndex,
	}, nil
}

func isBlobPath(path string) bool {
	return len(path) > 13 && path[:13] == "blobs/sha256/"
}

func digestFromBlobPath(path string) digest.Digest {
	if isBlobPath(path) {
		return digest.NewDigestFromEncoded(digest.SHA256, path[13:])
	}
	return ""
}

func isTarDiff(data []byte) bool {
	magic := []byte{'t', 'a', 'r', 'd', 'f', '1', '\n', 0}
	if len(data) < len(magic) {
		return false
	}
	for i, b := range magic {
		if data[i] != b {
			return false
		}
	}
	return true
}

func writeTarMember(w *tar.Writer, header *tar.Header, data []byte) error {
	if err := w.WriteHeader(header); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func writeTarFile(w *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name: name,
		Mode: 0644,
		Size: int64(len(data)),
	}
	return writeTarMember(w, header, data)
}

func writeTarFileFromReader(w *tar.Writer, name string, size int64, r io.Reader) error {
	header := &tar.Header{
		Name: name,
		Mode: 0644,
		Size: size,
	}
	if err := w.WriteHeader(header); err != nil {
		return err
	}
	_, err := io.Copy(w, r)
	return err
}

func writeTarFileFromFile(w *tar.Writer, name string, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	return writeTarFileFromReader(w, name, info.Size(), f)
}

func copyTarEntry(w *tar.Writer, header *tar.Header, r io.Reader) error {
	if err := w.WriteHeader(header); err != nil {
		return err
	}
	_, err := io.CopyN(w, r, header.Size)
	return err
}

func writeBlobTarFile(w *tar.Writer, tarIndex *TarIndex, d digest.Digest) error {
	name := blobTarName(d)
	size, err := tarIndex.GetSize(name)
	if err != nil {
		return err
	}
	r, err := tarIndex.GetReader(name)
	if err != nil {
		return err
	}
	return writeTarFileFromReader(w, name, size, r)
}

func blobTarName(d digest.Digest) string {
	return "blobs/sha256/" + d.Encoded()
}

func computeDigest(data []byte) digest.Digest {
	return digest.FromBytes(data)
}

func computeFileDigest(path string) (digest.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return digest.NewDigestFromBytes(digest.SHA256, h.Sum(nil)), nil
}
