//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
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
// Authentication relies on Azure default credentials (environment variables, managed identity, etc.)
func NewClient(ctx context.Context, config *config.AzblobArchiver) (Client, error) {
	// For now, we will use environment-based authentication
	// Users should set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT+AZURE_STORAGE_KEY
	clientDelegate, err := newDefaultClientDelegate(ctx)
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
	fmt.Println("container initialized", container)
	blob := container.Blob(formatBlobPath(URI.Path()) + "/" + fileName)
	fmt.Println("blob identified", blob)
	_, err := blob.Upload(ctx, bytes.NewReader(file), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{})
	return err
}

// Exist checks if a container or a blob exists
// If fileName is empty, then 'Exist' function will only check if the given container exists.
func (s *storageWrapper) Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error) {
	container := s.client.Container(URI.Hostname())

	// Check if container exists
	_, err := container.GetProperties(ctx, azblob.LeaseAccessConditions{})
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
	_, err = blob.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
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

	downloadResponse, err := blob.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}

	defer func() {
		if downloadResponse.Body(azblob.RetryReaderOptions{}) != nil {
			downloadResponse.Body(azblob.RetryReaderOptions{}).Close()
		}
	}()

	return io.ReadAll(downloadResponse.Body(azblob.RetryReaderOptions{}))
}

// Query retrieves blob names by provided prefix
func (s *storageWrapper) Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error) {
	fileNames := make([]string, 0)
	container := s.client.Container(URI.Hostname())

	prefix := formatBlobPath(URI.Path()) + "/" + fileNamePrefix

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			return nil, err
		}

		for _, blobInfo := range listBlob.Segment.BlobItems {
			fileNames = append(fileNames, blobInfo.Name)
		}

		marker = listBlob.NextMarker
	}

	return fileNames, nil
}

// QueryWithFilters retrieves blob names that match filter parameters. PageSize is optional, 0 means all records.
func (s *storageWrapper) QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error) {
	currentPos := offset
	resultSet := make([]string, 0)
	container := s.client.Container(URI.Hostname())

	prefix := formatBlobPath(URI.Path()) + "/" + fileNamePrefix

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			return nil, false, currentPos, err
		}

		for _, blobInfo := range listBlob.Segment.BlobItems {
			if completed := isPageCompleted(pageSize, len(resultSet)); completed {
				return resultSet, false, currentPos, nil
			}

			valid := true
			for _, f := range filters {
				if valid = f(blobInfo.Name); !valid {
					break
				}
			}

			if valid {
				if offset > 0 {
					offset--
					continue
				}
				// If match criteria and current cursor position is the last known position (offset is zero), append fileName to resultSet
				resultSet = append(resultSet, blobInfo.Name)
				currentPos++
			}
		}

		marker = listBlob.NextMarker
	}

	return resultSet, true, currentPos, nil
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
	if azErr, ok := err.(azblob.StorageError); ok {
		return azErr.ServiceCode() == azblob.ServiceCodeContainerNotFound
	}
	return false
}

func isBlobNotFoundError(err error) bool {
	if azErr, ok := err.(azblob.StorageError); ok {
		return azErr.ServiceCode() == azblob.ServiceCodeBlobNotFound
	}
	return false
}
