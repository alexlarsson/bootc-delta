package ocidelta

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- TarIndex tests ---

func createTestTar(t *testing.T, files map[string][]byte) string {
	t.Helper()
	tmp, err := os.CreateTemp("", "tarindex-test-*.tar")
	if err != nil {
		t.Fatal(err)
	}
	w, err := newTarOCIWriter(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := w.WriteFile(name, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return tmp.Name()
}

func TestTarIndexReadFile(t *testing.T) {
	files := map[string][]byte{
		"index.json":         []byte(`{"schemaVersion":2}`),
		"blobs/sha256/abc123": []byte("blob content here"),
	}
	path := createTestTar(t, files)
	defer os.Remove(path)

	idx, err := indexTarArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for name, want := range files {
		r, size, err := idx.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", name, err)
		}
		if size != int64(len(want)) {
			t.Errorf("ReadFile(%q) size = %d, want %d", name, size, len(want))
		}
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatalf("ReadAll(%q): %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("ReadFile(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestTarIndexReadFileMissing(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"a": []byte("x")})
	defer os.Remove(path)

	idx, err := indexTarArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	_, _, err = idx.ReadFile("nonexistent")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestTarIndexSeekable(t *testing.T) {
	data := []byte("seekable content")
	path := createTestTar(t, map[string][]byte{"file.txt": data})
	defer os.Remove(path)

	idx, err := indexTarArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	r, _, err := idx.ReadFile("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 4)
	r.Read(buf)
	if string(buf) != "seek" {
		t.Errorf("first read = %q, want \"seek\"", buf)
	}

	r.Seek(0, io.SeekStart)
	buf2 := make([]byte, 4)
	r.Read(buf2)
	if string(buf2) != "seek" {
		t.Errorf("after seek = %q, want \"seek\"", buf2)
	}
}

func TestTarIndexInvalidArchive(t *testing.T) {
	tmp, _ := os.CreateTemp("", "bad-tar-*")
	tmp.Write([]byte("not a tar file"))
	tmp.Close()
	defer os.Remove(tmp.Name())

	_, err := indexTarArchive(tmp.Name())
	if err == nil {
		t.Error("expected error for invalid tar")
	}
}

func TestTarIndexNotFound(t *testing.T) {
	_, err := indexTarArchive("/nonexistent/path.tar")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

// --- DirOCIReader tests ---

func TestDirOCIReaderReadFile(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "blobs", "sha256"), 0755)
	os.WriteFile(filepath.Join(dir, "index.json"), []byte(`{"v":1}`), 0644)
	os.WriteFile(filepath.Join(dir, "blobs", "sha256", "abc"), []byte("blob"), 0644)

	reader := NewDirOCIReader(dir)

	r, size, err := reader.ReadFile("index.json")
	if err != nil {
		t.Fatal(err)
	}
	if size != 7 {
		t.Errorf("size = %d, want 7", size)
	}
	data, _ := io.ReadAll(r)
	r.Close()
	if string(data) != `{"v":1}` {
		t.Errorf("content = %q", data)
	}

	r, size, err = reader.ReadFile("blobs/sha256/abc")
	if err != nil {
		t.Fatal(err)
	}
	if size != 4 {
		t.Errorf("size = %d, want 4", size)
	}
	data, _ = io.ReadAll(r)
	r.Close()
	if string(data) != "blob" {
		t.Errorf("content = %q", data)
	}
}

func TestDirOCIReaderMissing(t *testing.T) {
	reader := NewDirOCIReader(t.TempDir())
	_, _, err := reader.ReadFile("no-such-file")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

// --- tarOCIWriter tests ---

func TestTarOCIWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	files := map[string][]byte{
		"index.json":              []byte(`{"schemaVersion":2}`),
		"blobs/sha256/deadbeef":   []byte("layer data"),
		"blobs/sha256/cafebabe":   []byte("another blob"),
	}

	w, err := newTarOCIWriter(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := w.WriteFile(name, data); err != nil {
			t.Fatalf("WriteFile(%q): %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	idx, err := indexTarArchive(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for name, want := range files {
		r, _, err := idx.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", name, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("ReadFile(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestTarOCIWriterFromReader(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	w, err := newTarOCIWriter(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	content := "streamed content"
	if err := w.WriteFileFromReader("blobs/sha256/streamed", int64(len(content)), strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	idx, err := indexTarArchive(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	r, _, err := idx.ReadFile("blobs/sha256/streamed")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != content {
		t.Errorf("got %q, want %q", got, content)
	}
}

func TestTarOCIWriterParentDirs(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	w, err := newTarOCIWriter(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	w.WriteFile("a/b/c/file.txt", []byte("deep"))
	w.WriteFile("a/b/other.txt", []byte("shallow"))
	w.Close()

	idx, err := indexTarArchive(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Parent dirs should exist as entries
	for _, dirName := range []string{"a/", "a/b/", "a/b/c/"} {
		if _, _, err := idx.ReadFile(dirName); err != nil {
			t.Errorf("missing parent dir entry %q", dirName)
		}
	}
}

// --- dirOCIWriter tests ---

func TestDirOCIWriterWriteFile(t *testing.T) {
	dir := t.TempDir()
	w, err := newDirOCIWriter(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteFile("blobs/sha256/test", []byte("content")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "output", "blobs", "sha256", "test"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "content" {
		t.Errorf("got %q, want \"content\"", got)
	}
}

func TestDirOCIWriterFromReader(t *testing.T) {
	dir := t.TempDir()
	w, err := newDirOCIWriter(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	content := "from reader"
	if err := w.WriteFileFromReader("data.bin", int64(len(content)), strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "output", "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q, want %q", got, content)
	}
}

// --- readAll helper ---

func TestReadAll(t *testing.T) {
	reader := newMemoryReader(map[string][]byte{
		"test.txt": []byte("hello"),
	})
	data, err := readAll(reader, "test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want \"hello\"", data)
	}

	_, err = readAll(reader, "missing")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

// --- OpenOCIReader/Writer dispatch ---

func TestOpenOCIReaderOCIDir(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.json"), []byte("{}"), 0644)

	reader, err := OpenOCIReader("oci:"+dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*DirOCIReader); !ok {
		t.Errorf("expected *DirOCIReader, got %T", reader)
	}
}

func TestOpenOCIReaderOCIArchive(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"index.json": []byte("{}")})
	defer os.Remove(path)

	reader, err := OpenOCIReader("oci-archive:"+path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*TarIndex); !ok {
		t.Errorf("expected *TarIndex, got %T", reader)
	}
}

func TestOpenOCIReaderDefaultArchive(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"index.json": []byte("{}")})
	defer os.Remove(path)

	reader, err := OpenOCIReader(path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*TarIndex); !ok {
		t.Errorf("expected *TarIndex for bare path, got %T", reader)
	}
}

func TestOpenOCIWriterTar(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter(filepath.Join(dir, "out.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*tarOCIWriter); !ok {
		t.Errorf("expected *tarOCIWriter, got %T", w)
	}
}

func TestOpenOCIWriterOCIArchive(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter("oci-archive:" + filepath.Join(dir, "out.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*tarOCIWriter); !ok {
		t.Errorf("expected *tarOCIWriter, got %T", w)
	}
}

func TestOpenOCIWriterOCIDir(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter("oci:" + filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*dirOCIWriter); !ok {
		t.Errorf("expected *dirOCIWriter, got %T", w)
	}
}
