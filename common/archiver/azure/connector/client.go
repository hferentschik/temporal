//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

var (
	// ErrBucketNotFound is non retryable error that is thrown when the container doesn't exist
	ErrBucketNotFound = errors.New("container not found")
	errObjectNotFound = errors.New("blob not found")
)

type (
	// Precondition is a function that allow you to filter a query result.
	// If subject match params conditions then return true, else return false.
	Precondition func(subject interface{}) bool

	// Client is a wrapper around Azure Blob Storage client library.
	// Provides a clean abstraction layer between Temporal archiver and Azure Blob Storage.
	Client interface {
		Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error
		Get(ctx context.Context, URI archiver.URI, file string) ([]byte, error)
		Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error)
		QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error)
		Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error)
		Exists(ctx context.Context, URI archiver.URI, fileName string) (bool, error) // Added for compatibility with history archiver
	}

	storageWrapper struct {
		client AzureBlobStorageClient
	}
)

// NewClient returns a Temporal Azure Blob Storage Client based on configuration.
// Container must be created beforehand, this library doesn't create the required container.
// Authentication supports config values with environment variable fallback
func NewClient(ctx context.Context, config *config.AzblobArchiver) (Client, error) {
	var azureConfig *Config
	
	// Create azure config from temporalite config if provided
	if config != nil && config.RegionName != "" {
		azureConfig = &Config{
			AccountName: config.RegionName, // Using RegionName as AccountName for compatibility
			TenantID:    config.TenantID,
		}
	}
	
	// Use config-based authentication with environment variable fallback
	clientDelegate, err := newDefaultClientDelegateWithConfig(ctx, azureConfig)
	return &storageWrapper{client: clientDelegate}, err
}

// NewClientWithParams returns an Azure Blob Storage Client based on input parameters for testing
func NewClientWithParams(clientD AzureBlobStorageClient) (Client, error) {
	return &storageWrapper{client: clientD}, nil
}

// Upload pushes a file to Azure Blob Storage container
// example:
// Upload(ctx, "as://my-container/temporal_archival/development", "45273645-fileName.history", fileBytes)
func (s *storageWrapper) Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error {
	container := s.client.Container(URI.Hostname())
	blob := container.Blob(formatBlobPath(URI.Path()) + "/" + fileName)
	return blob.Upload(ctx, bytes.NewReader(file))
}

// Exist checks if a container or a blob exists
// If fileName is empty, then 'Exist' function will only check if the given container exists.
func (s *storageWrapper) Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error) {
	container := s.client.Container(URI.Hostname())

	// Check if container exists
	err := container.GetProperties(ctx)
	if err != nil {
		if isContainerNotFoundError(err) {
			return false, ErrBucketNotFound
		}
		return false, err
	}

	// If only checking container existence
	if fileName == "" {
		return true, nil
	}

	// Check if blob exists
	blob := container.Blob(formatBlobPath(URI.Path()) + "/" + fileName)
	err = blob.GetProperties(ctx)
	if err != nil {
		if isBlobNotFoundError(err) {
			return false, errObjectNotFound
		}
		return false, err
	}

	return true, nil
}

// Exists is an alias for Exist to maintain compatibility with history archiver interface
func (s *storageWrapper) Exists(ctx context.Context, URI archiver.URI, fileName string) (bool, error) {
	return s.Exist(ctx, URI, fileName)
}

// Get retrieves a file from Azure Blob Storage
func (s *storageWrapper) Get(ctx context.Context, URI archiver.URI, fileName string) ([]byte, error) {
	container := s.client.Container(URI.Hostname())
	blob := container.Blob(formatBlobPath(URI.Path()) + "/" + fileName)

	var buf bytes.Buffer
	err := blob.Download(ctx, &buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Query retrieves blob names by provided prefix
// TODO: This needs to be implemented with the new Azure SDK
// For now, return an error indicating this functionality needs to be implemented
func (s *storageWrapper) Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error) {
	return nil, errors.New("Query functionality needs to be implemented with new Azure SDK")
}

// QueryWithFilters retrieves blob names that match filter parameters. PageSize is optional, 0 means all records.
// TODO: This needs to be implemented with the new Azure SDK  
// For now, return an error indicating this functionality needs to be implemented
func (s *storageWrapper) QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error) {
	return nil, false, 0, errors.New("QueryWithFilters functionality needs to be implemented with new Azure SDK")
}

func isPageCompleted(pageSize, currentPosition int) bool {
	return pageSize != 0 && currentPosition > 0 && pageSize <= currentPosition
}

func formatBlobPath(blobPath string) string {
	if strings.HasPrefix(blobPath, "/") {
		return blobPath[1:]
	}
	return blobPath
}

func isContainerNotFoundError(err error) bool {
	return bloberror.HasCode(err, bloberror.ContainerNotFound)
}

func isBlobNotFoundError(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}
