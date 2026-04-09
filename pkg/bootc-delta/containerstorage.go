package bootcdelta

import (
	"encoding/json"
	"fmt"

	"github.com/containers/storage"
	"github.com/containers/storage/types"
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
)

type containerStorageDataSource struct {
	tarpatch.DataSource
	store   storage.Store
	imageID string
}

func (s *containerStorageDataSource) Cleanup() error {
	_, err := s.store.UnmountImage(s.imageID, true)
	return err
}

func OpenContainerStorage(graphRoot string) (storage.Store, error) {
	storeOpts, err := types.DefaultStoreOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to get default store options: %w", err)
	}
	if graphRoot != "" {
		storeOpts.GraphRoot = graphRoot
	}

	store, err := storage.GetStore(storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open container storage: %w", err)
	}
	return store, nil
}

func findImageByConfigDigest(store storage.Store, configDigest string, log Logger) (string, error) {
	images, err := store.Images()
	if err != nil {
		return "", fmt.Errorf("failed to list images: %w", err)
	}

	log.Debug("Found %d images in container storage", len(images))

	for _, img := range images {
		manifestData, err := store.ImageBigData(img.ID, "manifest")
		if err != nil {
			continue
		}

		var manifest struct {
			Config struct {
				Digest string `json:"digest"`
			} `json:"config"`
		}
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			continue
		}

		log.Debug("  Image %s: config digest %s", img.ID[:16], manifest.Config.Digest)

		if manifest.Config.Digest == configDigest {
			log.Debug("Matched image: %s", img.ID[:16])
			return img.ID, nil
		}
	}

	return "", fmt.Errorf("no image found with config digest %s", configDigest)
}

func resolveContainerStorageDataSource(store storage.Store, sourceConfigDigest string, log Logger) (*containerStorageDataSource, error) {
	imageID, err := findImageByConfigDigest(store, sourceConfigDigest, log)
	if err != nil {
		return nil, err
	}

	mountPoint, err := store.MountImage(imageID, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to mount image %s: %w", imageID[:16], err)
	}

	log.Debug("Mounted image at %s", mountPoint)

	return &containerStorageDataSource{
		DataSource: tarpatch.NewFilesystemDataSource(mountPoint),
		store:      store,
		imageID:    imageID,
	}, nil
}
