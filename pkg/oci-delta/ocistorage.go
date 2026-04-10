package ocidelta

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	cpimage "github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/transports/alltransports"
)

type OCIReader interface {
	ReadFile(name string) (io.ReadSeekCloser, int64, error)
	Close() error
}

type OCIWriter interface {
	WriteFile(name string, data []byte) error
	WriteFileFromReader(name string, size int64, r io.Reader) error
	Close() error
}

func OpenOCIReader(ref string, tmpDir string) (OCIReader, error) {
	if strings.HasPrefix(ref, "containers-storage:") {
		return newCSReader(ref, tmpDir)
	}
	if strings.HasPrefix(ref, "oci-archive:") {
		return indexTarArchive(ref[len("oci-archive:"):])
	}
	if strings.HasPrefix(ref, "oci:") {
		return NewDirOCIReader(ref[len("oci:"):]), nil
	}
	return indexTarArchive(ref)
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

func readAll(reader OCIReader, name string) ([]byte, error) {
	r, _, err := reader.ReadFile(name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// TarIndex — tar archive backed OCIReader

type TarIndex struct {
	file    *os.File
	entries map[string]*TarEntry
}

type TarEntry struct {
	offset int64
	size   int64
}

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (readSeekNopCloser) Close() error { return nil }

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

		dataOffset := tracker.offset

		entries[hdr.Name] = &TarEntry{
			offset: dataOffset,
			size:   hdr.Size,
		}

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

// DirOCIReader — directory backed OCIReader

type DirOCIReader struct {
	dir string
}

func NewDirOCIReader(dir string) *DirOCIReader {
	return &DirOCIReader{dir: dir}
}

func (d *DirOCIReader) ReadFile(name string) (io.ReadSeekCloser, int64, error) {
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

func (d *DirOCIReader) Close() error {
	return nil
}

// tarOCIWriter — tar archive backed OCIWriter

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

// dirOCIWriter — directory backed OCIWriter

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

// csOCIReader — container storage backed OCIReader (via temp OCI directory)

type csOCIReader struct {
	*DirOCIReader
	tmpDir string
}

func newCSReader(ref string, tmpDir string) (*csOCIReader, error) {
	ociDir, err := os.MkdirTemp(tmpDir, "cs-reader-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	if err := copyImage(context.Background(), ref, "oci:"+ociDir); err != nil {
		os.RemoveAll(ociDir)
		return nil, fmt.Errorf("failed to copy from container storage: %w", err)
	}

	return &csOCIReader{
		DirOCIReader: NewDirOCIReader(ociDir),
		tmpDir:       ociDir,
	}, nil
}

func (r *csOCIReader) Close() error {
	return os.RemoveAll(r.tmpDir)
}

func copyImage(ctx context.Context, srcName, destName string) error {
	srcRef, err := alltransports.ParseImageName(srcName)
	if err != nil {
		return fmt.Errorf("invalid source reference %q: %w", srcName, err)
	}

	destRef, err := alltransports.ParseImageName(destName)
	if err != nil {
		return fmt.Errorf("invalid destination reference %q: %w", destName, err)
	}

	policy := &signature.Policy{Default: []signature.PolicyRequirement{signature.NewPRInsecureAcceptAnything()}}
	pc, err := signature.NewPolicyContext(policy)
	if err != nil {
		return err
	}
	defer pc.Destroy()

	_, err = cpimage.Image(ctx, pc, destRef, srcRef, nil)
	return err
}
