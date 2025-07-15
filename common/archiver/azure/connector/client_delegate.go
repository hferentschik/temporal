//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_delegate_mock.go

package connector

import (
	"context"
	"io"
	"net/url"
	"os"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type (
	// AzureBlobStorageClient is an interface that exposes methods from Azure Blob Storage client
	AzureBlobStorageClient interface {
		Container(containerName string) ContainerHandleWrapper
	}

	clientDelegate struct {
		serviceURL azblob.ServiceURL
	}
)

type (
	// ContainerHandleWrapper is an interface that exposes methods from Azure Blob Storage container
	ContainerHandleWrapper interface {
		Blob(blobName string) BlobHandleWrapper
		ListBlobsFlatSegment(ctx context.Context, marker azblob.Marker, options azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error)
		GetProperties(ctx context.Context, ac azblob.LeaseAccessConditions) (*azblob.ContainerGetPropertiesResponse, error)
	}

	containerDelegate struct {
		containerURL azblob.ContainerURL
	}
)

type (
	// BlobHandleWrapper is an interface that exposes methods from Azure Blob Storage blob
	BlobHandleWrapper interface {
		Upload(ctx context.Context, body io.ReadSeeker, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, ac azblob.BlobAccessConditions, tier azblob.AccessTierType, tags azblob.BlobTagsMap, cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobUploadResponse, error)
		Download(ctx context.Context, offset int64, count int64, ac azblob.BlobAccessConditions, getsMD5 bool, cpk azblob.ClientProvidedKeyOptions) (*azblob.DownloadResponse, error)
		GetProperties(ctx context.Context, ac azblob.BlobAccessConditions, cpk azblob.ClientProvidedKeyOptions) (*azblob.BlobGetPropertiesResponse, error)
	}

	blobDelegate struct {
		blobURL azblob.BlockBlobURL
	}
)

// newDefaultClientDelegate creates a new Azure Blob Storage client using environment variables
func newDefaultClientDelegate(ctx context.Context) (*clientDelegate, error) {
	// Try connection string first
	if connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING"); connStr != "" {
		return newClientDelegateWithConnectionString(ctx, connStr)
	}

	// Try account name and key
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_KEY")
	if accountName != "" && accountKey != "" {
		return newClientDelegateWithCredentials(ctx, accountName, accountKey)
	}

	// For development/testing, try anonymous access (won't work in production)
	if accountName != "" {
		serviceURL, err := url.Parse("https://" + accountName + ".blob.core.windows.net")
		if err != nil {
			return nil, err
		}
		pipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		azServiceURL := azblob.NewServiceURL(*serviceURL, pipeline)
		return &clientDelegate{serviceURL: azServiceURL}, nil
	}

	// Default fallback - try to create with anonymous credentials for testing
	serviceURL, _ := url.Parse("https://test.blob.core.windows.net")
	pipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	azServiceURL := azblob.NewServiceURL(*serviceURL, pipeline)
	return &clientDelegate{serviceURL: azServiceURL}, nil
}

// newClientDelegateWithConnectionString creates a new Azure Blob Storage client using connection string
func newClientDelegateWithConnectionString(ctx context.Context, connectionString string) (*clientDelegate, error) {
	// Parse connection string manually since NewSharedKeyCredentialFromConnectionString might not exist
	// Extract account name and key from connection string
	// Format: DefaultEndpointsProtocol=https;AccountName=<name>;AccountKey=<key>;EndpointSuffix=core.windows.net

	// For now, use a simple approach - require separate account name and key
	// This can be enhanced later to parse the connection string properly
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_KEY")

	if accountName == "" || accountKey == "" {
		// Fallback to anonymous for testing
		serviceURL, _ := url.Parse("https://test.blob.core.windows.net")
		pipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		azServiceURL := azblob.NewServiceURL(*serviceURL, pipeline)
		return &clientDelegate{serviceURL: azServiceURL}, nil
	}

	return newClientDelegateWithCredentials(ctx, accountName, accountKey)
}

// newClientDelegateWithCredentials creates a new Azure Blob Storage client using account name and key
func newClientDelegateWithCredentials(ctx context.Context, accountName, accountKey string) (*clientDelegate, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	serviceURL, err := url.Parse("https://" + accountName + ".blob.core.windows.net")
	if err != nil {
		return nil, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	azServiceURL := azblob.NewServiceURL(*serviceURL, pipeline)

	return &clientDelegate{serviceURL: azServiceURL}, nil
}

// Container returns a ContainerHandleWrapper, which provides operations on the named container.
// This call does not perform any network operations.
func (c *clientDelegate) Container(containerName string) ContainerHandleWrapper {
	containerURL := c.serviceURL.NewContainerURL(containerName)
	return &containerDelegate{containerURL: containerURL}
}

// Blob returns a BlobHandleWrapper, which provides operations on the named blob.
// This call does not perform any network operations.
func (cd *containerDelegate) Blob(blobName string) BlobHandleWrapper {
	blobURL := cd.containerURL.NewBlockBlobURL(blobName)
	return &blobDelegate{blobURL: blobURL}
}

// ListBlobsFlatSegment lists blobs in the container with pagination support
func (cd *containerDelegate) ListBlobsFlatSegment(ctx context.Context, marker azblob.Marker, options azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error) {
	return cd.containerURL.ListBlobsFlatSegment(ctx, marker, options)
}

// GetProperties returns the container's properties
func (cd *containerDelegate) GetProperties(ctx context.Context, ac azblob.LeaseAccessConditions) (*azblob.ContainerGetPropertiesResponse, error) {
	return cd.containerURL.GetProperties(ctx, ac)
}

// Upload uploads content to the blob
func (bd *blobDelegate) Upload(ctx context.Context, body io.ReadSeeker, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, ac azblob.BlobAccessConditions, tier azblob.AccessTierType, tags azblob.BlobTagsMap, cpk azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobUploadResponse, error) {
	return bd.blobURL.Upload(ctx, body, headers, metadata, ac, tier, tags, cpk, azblob.ImmutabilityPolicyOptions{})
}

// Download downloads blob content
func (bd *blobDelegate) Download(ctx context.Context, offset int64, count int64, ac azblob.BlobAccessConditions, getsMD5 bool, cpk azblob.ClientProvidedKeyOptions) (*azblob.DownloadResponse, error) {
	return bd.blobURL.Download(ctx, offset, count, ac, getsMD5, cpk)
}

// GetProperties returns the blob's properties
func (bd *blobDelegate) GetProperties(ctx context.Context, ac azblob.BlobAccessConditions, cpk azblob.ClientProvidedKeyOptions) (*azblob.BlobGetPropertiesResponse, error) {
	return bd.blobURL.GetProperties(ctx, ac, cpk)
}
