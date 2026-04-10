package ocidelta

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Logger interface {
	Debug(format string, args ...interface{})
	Warning(format string, args ...interface{})
}

var ociLayoutFileData = []byte(`{"imageLayoutVersion":"1.0.0"}`)

type BlobStore interface {
	ReadFile(name string) (io.ReadSeekCloser, int64, error)
	Close() error
}

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (readSeekNopCloser) Close() error { return nil }

type DirBlobStore struct {
	dir string
}

func NewDirBlobStore(dir string) *DirBlobStore {
	return &DirBlobStore{dir: dir}
}

func (d *DirBlobStore) ReadFile(name string) (io.ReadSeekCloser, int64, error) {
	f, err := os.Open(d.dir + "/" + name)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

func (d *DirBlobStore) Close() error {
	return nil
}

type OCIWriter interface {
	WriteFile(name string, data []byte) error
	WriteFileFromReader(name string, size int64, r io.Reader) error
	Close() error
}

type tarOCIWriter struct {
	file *os.File
	tw   *tar.Writer
	dirs map[string]bool
}

func newTarOCIWriter(path string) (*tarOCIWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(f)
	return &tarOCIWriter{file: f, tw: tw, dirs: make(map[string]bool)}, nil
}

func (w *tarOCIWriter) ensureParentDirs(name string) error {
	parts := strings.Split(name, "/")
	for i := 1; i < len(parts); i++ {
		dir := strings.Join(parts[:i], "/") + "/"
		if !w.dirs[dir] {
			if err := writeTarDir(w.tw, dir); err != nil {
				return err
			}
			w.dirs[dir] = true
		}
	}
	return nil
}

func (w *tarOCIWriter) WriteFile(name string, data []byte) error {
	if err := w.ensureParentDirs(name); err != nil {
		return err
	}
	return writeTarFile(w.tw, name, data)
}

func (w *tarOCIWriter) WriteFileFromReader(name string, size int64, r io.Reader) error {
	if err := w.ensureParentDirs(name); err != nil {
		return err
	}
	return writeTarFileFromReader(w.tw, name, size, r)
}

func (w *tarOCIWriter) Close() error {
	err := w.tw.Close()
	if err2 := w.file.Close(); err == nil {
		err = err2
	}
	return err
}

type dirOCIWriter struct {
	dir string
}

func newDirOCIWriter(dir string) (*dirOCIWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &dirOCIWriter{dir: dir}, nil
}

func (w *dirOCIWriter) WriteFile(name string, data []byte) error {
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (w *dirOCIWriter) WriteFileFromReader(name string, size int64, r io.Reader) error {
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (w *dirOCIWriter) Close() error {
	return nil
}

func OpenOCIWriter(ref string) (OCIWriter, error) {
	if strings.HasPrefix(ref, "oci-archive:") {
		return newTarOCIWriter(ref[len("oci-archive:"):])
	}
	if strings.HasPrefix(ref, "oci:") {
		return newDirOCIWriter(ref[len("oci:"):])
	}
	return newTarOCIWriter(ref)
}

func OpenBlobStore(ref string) (BlobStore, error) {
	if strings.HasPrefix(ref, "oci-archive:") {
		return indexTarArchive(ref[len("oci-archive:"):])
	}
	if strings.HasPrefix(ref, "oci:") {
		return NewDirBlobStore(ref[len("oci:"):]), nil
	}
	return indexTarArchive(ref)
}

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
	blobStore      BlobStore
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

func (idx *TarIndex) ReadFile(name string) (io.ReadSeekCloser, int64, error) {
	entry, ok := idx.entries[name]
	if !ok {
		return nil, 0, fmt.Errorf("file not found in tar: %s", name)
	}

	return readSeekNopCloser{io.NewSectionReader(idx.file, entry.offset, entry.size)}, entry.size, nil
}

func (idx *TarIndex) Close() error {
	if idx.file != nil {
		return idx.file.Close()
	}
	return nil
}

func readAllFromStore(store BlobStore, name string) ([]byte, error) {
	r, _, err := store.ReadFile(name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func parseOCIImage(blobStore BlobStore) (*OCIImage, error) {
	indexData, err := readAllFromStore(blobStore, "index.json")
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

	manifestData, err := readAllFromStore(blobStore, blobTarName(manifestDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest v1.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	if manifest.Config.Digest == "" {
		return nil, fmt.Errorf("manifest has no config digest")
	}

	configData, err := readAllFromStore(blobStore, blobTarName(manifest.Config.Digest))
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
		blobStore:      blobStore,
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

func writeTarDir(w *tar.Writer, name string) error {
	return w.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir,
		Name:     name,
		Mode:     0755,
	})
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
