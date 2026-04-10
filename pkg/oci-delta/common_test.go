package ocidelta

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	digest "github.com/opencontainers/go-digest"
)

func TestBlobTarName(t *testing.T) {
	d := digest.FromBytes([]byte("hello"))
	got := blobTarName(d)
	want := "blobs/sha256/" + d.Encoded()
	if got != want {
		t.Errorf("blobTarName(%s) = %q, want %q", d, got, want)
	}
}

func TestIsBlobPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"blobs/sha256/abc123", true},
		{"blobs/sha256/a", true},
		{"blobs/sha256/", false},
		{"blobs/sha256", false},
		{"blobs/sha512/abc", false},
		{"index.json", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isBlobPath(tt.path); got != tt.want {
			t.Errorf("isBlobPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestDigestFromBlobPath(t *testing.T) {
	d := digest.FromBytes([]byte("test"))
	path := blobTarName(d)
	got := digestFromBlobPath(path)
	if got != d {
		t.Errorf("digestFromBlobPath(%q) = %s, want %s", path, got, d)
	}

	got = digestFromBlobPath("index.json")
	if got != "" {
		t.Errorf("digestFromBlobPath(\"index.json\") = %s, want empty", got)
	}
}

func TestIsTarDiff(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"valid magic", []byte{'t', 'a', 'r', 'd', 'f', '1', '\n', 0, 0xff}, true},
		{"exact magic", []byte{'t', 'a', 'r', 'd', 'f', '1', '\n', 0}, true},
		{"wrong magic", []byte{'t', 'a', 'r', 'd', 'f', '2', '\n', 0}, false},
		{"too short", []byte{'t', 'a', 'r'}, false},
		{"empty", []byte{}, false},
		{"gzip header", []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTarDiff(tt.data); got != tt.want {
				t.Errorf("isTarDiff(%v) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}

func TestComputeDigest(t *testing.T) {
	data := []byte("hello world")
	d := computeDigest(data)
	if d.Algorithm() != digest.SHA256 {
		t.Errorf("expected SHA256, got %s", d.Algorithm())
	}
	if err := d.Validate(); err != nil {
		t.Errorf("invalid digest: %v", err)
	}
	if computeDigest(data) != d {
		t.Error("same input produced different digests")
	}
	if computeDigest([]byte("other")) == d {
		t.Error("different input produced same digest")
	}
}

func TestComputeFileDigest(t *testing.T) {
	f, err := os.CreateTemp("", "digest-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	data := []byte("file content for hashing")
	f.Write(data)
	f.Close()

	got, err := computeFileDigest(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	want := computeDigest(data)
	if got != want {
		t.Errorf("computeFileDigest = %s, want %s", got, want)
	}
}

func TestComputeFileDigestNotFound(t *testing.T) {
	_, err := computeFileDigest("/nonexistent/path")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestParseOCIImage(t *testing.T) {
	configDigest, configData := makeBlob(t, `{
		"rootfs": {
			"type": "layers",
			"diff_ids": ["sha256:aaaa000000000000000000000000000000000000000000000000000000000000"]
		}
	}`)

	layerDigest := digest.FromBytes([]byte("layer-data"))

	manifestDigest, manifestData := makeBlob(t, `{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": "`+configDigest.String()+`",
			"size": `+itoa(len(configData))+`
		},
		"layers": [{
			"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
			"digest": "`+layerDigest.String()+`",
			"size": 100
		}]
	}`)

	indexData := []byte(`{
		"schemaVersion": 2,
		"manifests": [{
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"digest": "` + manifestDigest.String() + `",
			"size": ` + itoa(len(manifestData)) + `
		}]
	}`)

	reader := newMemoryReader(map[string][]byte{
		"index.json":              indexData,
		blobTarName(manifestDigest): manifestData,
		blobTarName(configDigest):   configData,
	})

	img, err := parseOCIImage(reader)
	if err != nil {
		t.Fatalf("parseOCIImage failed: %v", err)
	}

	if len(img.layers) != 1 {
		t.Fatalf("expected 1 layer, got %d", len(img.layers))
	}
	if img.layers[0].Digest != layerDigest {
		t.Errorf("layer digest = %s, want %s", img.layers[0].Digest, layerDigest)
	}
	if img.layers[0].DiffID.String() != "sha256:aaaa000000000000000000000000000000000000000000000000000000000000" {
		t.Errorf("unexpected diff_id: %s", img.layers[0].DiffID)
	}
	if img.configDigest != configDigest {
		t.Errorf("config digest = %s, want %s", img.configDigest, configDigest)
	}
	if img.manifestDigest != manifestDigest {
		t.Errorf("manifest digest = %s, want %s", img.manifestDigest, manifestDigest)
	}
	if _, ok := img.layerByDigest[layerDigest]; !ok {
		t.Error("layer not in layerByDigest map")
	}
	if _, ok := img.layerByDiffID[img.layers[0].DiffID]; !ok {
		t.Error("layer not in layerByDiffID map")
	}
}

func TestParseOCIImageNoManifests(t *testing.T) {
	reader := newMemoryReader(map[string][]byte{
		"index.json": []byte(`{"schemaVersion":2,"manifests":[]}`),
	})
	_, err := parseOCIImage(reader)
	if err == nil {
		t.Fatal("expected error for empty manifests")
	}
}

func TestParseOCIImageManifestList(t *testing.T) {
	reader := newMemoryReader(map[string][]byte{
		"index.json": []byte(`{
			"schemaVersion":2,
			"manifests":[{
				"mediaType":"application/vnd.oci.image.index.v1+json",
				"digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000",
				"size":100
			}]
		}`),
	})
	_, err := parseOCIImage(reader)
	if err == nil {
		t.Fatal("expected error for manifest list")
	}
}

func TestParseOCIImageInvalidJSON(t *testing.T) {
	reader := newMemoryReader(map[string][]byte{
		"index.json": []byte(`not json`),
	})
	_, err := parseOCIImage(reader)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseOCIImageMissingIndex(t *testing.T) {
	reader := newMemoryReader(map[string][]byte{})
	_, err := parseOCIImage(reader)
	if err == nil {
		t.Fatal("expected error for missing index.json")
	}
}

// --- test helpers ---

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

func makeBlob(t *testing.T, json string) (digest.Digest, []byte) {
	t.Helper()
	data := []byte(json)
	return digest.FromBytes(data), data
}

type memoryReader struct {
	files map[string][]byte
}

func newMemoryReader(files map[string][]byte) *memoryReader {
	return &memoryReader{files: files}
}

func (m *memoryReader) ReadFile(name string) (io.ReadSeekCloser, int64, error) {
	data, ok := m.files[name]
	if !ok {
		return nil, 0, fmt.Errorf("file not found: %s", name)
	}
	return readSeekNopCloser{bytes.NewReader(data)}, int64(len(data)), nil
}

func (m *memoryReader) Close() error {
	return nil
}
