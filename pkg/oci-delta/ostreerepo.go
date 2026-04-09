package ocidelta

// #cgo pkg-config: ostree-1
// #include <ostree.h>
// #include <gio/gio.h>
// #include <stdlib.h>
import "C"
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
)

type OstreeRepoDataSource struct {
	repoPath    string
	pathToObj   map[string]string // filesystem path → "ab/cd...rest" object path
	currentFile *os.File
}

func openOstreeRepo(repoPath string) (*C.OstreeRepo, error) {
	cPath := C.CString(repoPath)
	defer C.free(unsafe.Pointer(cPath))

	gfile := C.g_file_new_for_path(cPath)
	defer C.g_object_unref(C.gpointer(gfile))

	repo := C.ostree_repo_new(gfile)
	if repo == nil {
		return nil, fmt.Errorf("failed to create OstreeRepo")
	}

	var gerr *C.GError
	if C.ostree_repo_open(repo, nil, &gerr) == 0 {
		defer C.g_error_free(gerr)
		msg := C.GoString(gerr.message)
		C.g_object_unref(C.gpointer(repo))
		return nil, fmt.Errorf("failed to open ostree repo: %s", msg)
	}

	return repo, nil
}

func listOstreeRefs(repo *C.OstreeRepo, prefix string) ([]string, error) {
	cPrefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPrefix))

	var refsHash *C.GHashTable
	var gerr *C.GError
	if C.ostree_repo_list_refs(repo, cPrefix, &refsHash, nil, &gerr) == 0 {
		defer C.g_error_free(gerr)
		return nil, fmt.Errorf("failed to list refs: %s", C.GoString(gerr.message))
	}
	defer C.g_hash_table_unref(refsHash)

	var iter C.GHashTableIter
	var key, value C.gpointer
	C.g_hash_table_iter_init(&iter, refsHash)

	var refs []string
	for C.g_hash_table_iter_next(&iter, &key, &value) != 0 {
		suffix := C.GoString((*C.char)(key))
		refs = append(refs, prefix+"/"+suffix)
	}
	return refs, nil
}

func resolveRef(repo *C.OstreeRepo, ref string) (string, error) {
	cRef := C.CString(ref)
	defer C.free(unsafe.Pointer(cRef))

	var cRev *C.char
	var gerr *C.GError
	if C.ostree_repo_resolve_rev(repo, cRef, 0, &cRev, &gerr) == 0 {
		defer C.g_error_free(gerr)
		return "", fmt.Errorf("failed to resolve ref %s: %s", ref, C.GoString(gerr.message))
	}
	defer C.g_free(C.gpointer(cRev))
	return C.GoString(cRev), nil
}

func getCommitMetadataString(repo *C.OstreeRepo, ref string, key string) (string, error) {
	rev, err := resolveRef(repo, ref)
	if err != nil {
		return "", err
	}

	cRev := C.CString(rev)
	defer C.free(unsafe.Pointer(cRev))

	var commit *C.GVariant
	var gerr *C.GError
	if C.ostree_repo_load_commit(repo, cRev, &commit, nil, &gerr) == 0 {
		defer C.g_error_free(gerr)
		return "", fmt.Errorf("failed to load commit for ref %s: %s", ref, C.GoString(gerr.message))
	}
	defer C.g_variant_unref(commit)

	// Commit format is (a{sv}aya(say)sstayay), metadata is index 0
	metadata := C.g_variant_get_child_value(commit, 0)
	defer C.g_variant_unref(metadata)

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	val := C.g_variant_lookup_value(metadata, cKey, C.G_VARIANT_TYPE_STRING)
	if val == nil {
		return "", fmt.Errorf("metadata key %s not found in commit %s", key, ref)
	}
	defer C.g_variant_unref(val)

	return C.GoString(C.g_variant_get_string(val, nil)), nil
}

// enumerateCommitFiles recursively lists all regular files in the commit,
// returning a map from path (without leading /) to ostree object path.
func enumerateCommitFiles(repo *C.OstreeRepo, ref string) (map[string]string, error) {
	cRef := C.CString(ref)
	defer C.free(unsafe.Pointer(cRef))

	var root *C.GFile
	var gerr *C.GError
	if C.ostree_repo_read_commit(repo, cRef, &root, nil, nil, &gerr) == 0 {
		defer C.g_error_free(gerr)
		return nil, fmt.Errorf("failed to read commit %s: %s", ref, C.GoString(gerr.message))
	}
	defer C.g_object_unref(C.gpointer(root))

	result := make(map[string]string)
	if err := enumerateDir(root, "", result); err != nil {
		return nil, err
	}
	return result, nil
}

func enumerateDir(dir *C.GFile, prefix string, result map[string]string) error {
	cAttrs := C.CString("standard::name,standard::type")
	defer C.free(unsafe.Pointer(cAttrs))

	var gerr *C.GError
	enumerator := C.g_file_enumerate_children(dir, cAttrs, C.G_FILE_QUERY_INFO_NOFOLLOW_SYMLINKS, nil, &gerr)
	if enumerator == nil {
		defer C.g_error_free(gerr)
		return fmt.Errorf("failed to enumerate %s: %s", prefix, C.GoString(gerr.message))
	}
	defer C.g_object_unref(C.gpointer(enumerator))

	for {
		info := C.g_file_enumerator_next_file(enumerator, nil, &gerr)
		if info == nil {
			if gerr != nil {
				defer C.g_error_free(gerr)
				return fmt.Errorf("error enumerating %s: %s", prefix, C.GoString(gerr.message))
			}
			break
		}

		name := C.GoString(C.g_file_info_get_name(info))
		fileType := C.g_file_info_get_file_type(info)
		C.g_object_unref(C.gpointer(info))

		var path string
		if prefix == "" {
			path = name
		} else {
			path = prefix + "/" + name
		}

		cName := C.CString(name)
		child := C.g_file_get_child(dir, cName)
		C.free(unsafe.Pointer(cName))

		if fileType == C.G_FILE_TYPE_DIRECTORY {
			err := enumerateDir(child, path, result)
			C.g_object_unref(C.gpointer(child))
			if err != nil {
				return err
			}
		} else if fileType == C.G_FILE_TYPE_REGULAR {
			repoFile := ((*C.OstreeRepoFile)(unsafe.Pointer(child)))

			var resolveErr *C.GError
			C.ostree_repo_file_ensure_resolved(repoFile, &resolveErr)
			if resolveErr != nil {
				C.g_error_free(resolveErr)
				C.g_object_unref(C.gpointer(child))
				continue
			}

			checksum := C.GoString(C.ostree_repo_file_get_checksum(repoFile))
			C.g_object_unref(C.gpointer(child))

			if len(checksum) >= 2 {
				result[path] = checksum[:2] + "/" + checksum[2:] + ".file"
			}
		} else {
			C.g_object_unref(C.gpointer(child))
		}
	}
	return nil
}

func NewOstreeRepoDataSource(repoPath string, ref string, log Logger) (*OstreeRepoDataSource, error) {
	log.Debug("Building file index from ostree ref %s", ref)

	repo, err := openOstreeRepo(repoPath)
	if err != nil {
		return nil, err
	}
	defer C.g_object_unref(C.gpointer(repo))

	pathToObj, err := enumerateCommitFiles(repo, ref)
	if err != nil {
		return nil, err
	}

	log.Debug("Indexed %d files from ostree ref", len(pathToObj))

	return &OstreeRepoDataSource{
		repoPath:  repoPath,
		pathToObj: pathToObj,
	}, nil
}

func (s *OstreeRepoDataSource) SetCurrentFile(file string) error {
	if s.currentFile != nil {
		s.currentFile.Close()
		s.currentFile = nil
	}

	objPath, ok := s.pathToObj[file]
	if !ok && strings.HasPrefix(file, "etc/") {
		objPath, ok = s.pathToObj["usr/"+file]
	}
	if !ok {
		return fmt.Errorf("file not found in ostree ref: %s", file)
	}

	f, err := os.Open(filepath.Join(s.repoPath, "objects", objPath))
	if err != nil {
		return fmt.Errorf("failed to open ostree object for %s: %w", file, err)
	}
	s.currentFile = f
	return nil
}

func (s *OstreeRepoDataSource) Read(data []byte) (int, error) {
	if s.currentFile == nil {
		return 0, fmt.Errorf("no current file set")
	}
	return s.currentFile.Read(data)
}

func (s *OstreeRepoDataSource) Seek(offset int64, whence int) (int64, error) {
	if s.currentFile == nil {
		return 0, fmt.Errorf("no current file set")
	}
	return s.currentFile.Seek(offset, whence)
}

func (s *OstreeRepoDataSource) Close() error {
	if s.currentFile != nil {
		err := s.currentFile.Close()
		s.currentFile = nil
		return err
	}
	return nil
}

var _ tarpatch.DataSource = (*OstreeRepoDataSource)(nil)

func findOstreeRefByConfig(repoPath string, sourceConfigDigest string, log Logger) (string, error) {
	log.Debug("Looking for ostree ref with config digest %s", sourceConfigDigest)

	repo, err := openOstreeRepo(repoPath)
	if err != nil {
		return "", err
	}
	defer C.g_object_unref(C.gpointer(repo))

	refs, err := listOstreeRefs(repo, "ostree/container/image")
	if err != nil {
		return "", err
	}
	if len(refs) == 0 {
		return "", fmt.Errorf("no container image refs found in ostree repo")
	}

	log.Debug("Found %d container image refs", len(refs))

	for _, ref := range refs {
		manifestStr, err := getCommitMetadataString(repo, ref, "ostree.manifest")
		if err != nil {
			continue
		}

		var manifest struct {
			Config struct {
				Digest string `json:"digest"`
			} `json:"config"`
		}
		if err := json.Unmarshal([]byte(manifestStr), &manifest); err != nil {
			continue
		}

		log.Debug("  Ref %s: config digest %s", ref, manifest.Config.Digest)

		if manifest.Config.Digest == sourceConfigDigest {
			log.Debug("Matched ref: %s", ref)
			return ref, nil
		}
	}

	return "", fmt.Errorf("no ostree ref found with config digest %s", sourceConfigDigest)
}

func resolveOstreeDataSource(repoPath string, sourceConfigDigest string, log Logger) (*OstreeRepoDataSource, error) {
	ref, err := findOstreeRefByConfig(repoPath, sourceConfigDigest, log)
	if err != nil {
		return nil, err
	}

	return NewOstreeRepoDataSource(repoPath, ref, log)
}

