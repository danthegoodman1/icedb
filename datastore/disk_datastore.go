package datastore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type (
	DiskDataStore struct {
		rootPath string
	}
)

func NewDiskDataStore(rootPath string) (*DiskDataStore, error) {
	dds := &DiskDataStore{
		rootPath: rootPath,
	}

	return dds, nil
}

func (dds *DiskDataStore) GetColumnFile(_ context.Context, table, partID, column string) (io.Reader, error) {
	f, err := os.Open(filepath.Join(dds.rootPath, table, partID, column+".bin"))
	if err != nil {
		return nil, fmt.Errorf("error in os.Open: %w", err)
	}
	return f, err
}

func (dds *DiskDataStore) WriteColumnFile(_ context.Context, table, partID, column string) (io.Writer, error) {
	f, err := os.Create(filepath.Join(dds.rootPath, table, partID, column+".bin"))
	if err != nil {
		return nil, fmt.Errorf("error in os.Create: %w", err)
	}
	return f, err
}

func (dds *DiskDataStore) Shutdown() error {
	return nil
}
