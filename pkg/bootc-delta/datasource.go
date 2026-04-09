package bootcdelta

import (
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
)

type deltaDataSource interface {
	tarpatch.DataSource
	Cleanup() error
}

type simpleDataSource struct {
	tarpatch.DataSource
}

func (s *simpleDataSource) Cleanup() error {
	return nil
}
