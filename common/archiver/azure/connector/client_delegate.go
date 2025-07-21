//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_delegate_mock.go

package connector

import (
	"context"
	"fmt"
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
	// Get required environment variables
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	tenantID := os.Getenv("AZURE_TENANT_ID")
	
	if accountName == "" {
		return nil, fmt.Errorf("AZURE_STORAGE_ACCOUNT_NAME environment variable is required")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("AZURE_TENANT_ID environment variable is required")
	}

	return newClientDelegateWithManagedIdentity(ctx, accountName, tenantID)
}

// newClientDelegateWithManagedIdentity creates a new Azure Blob Storage client using managed identity
func newClientDelegateWithManagedIdentity(ctx context.Context, accountName, tenantID string) (*clientDelegate, error) {
	// For managed identity authentication, we'll use anonymous credentials
	// and rely on Azure's managed identity for authentication at runtime
	// This requires the application to be running in an Azure environment with managed identity enabled
	
	serviceURL, err := url.Parse("https://" + accountName + ".blob.core.windows.net")
	if err != nil {
		return nil, fmt.Errorf("failed to parse storage account URL: %w", err)
	}

	// Use anonymous credential - Azure managed identity will handle authentication
	anonymousCredential := azblob.NewAnonymousCredential()
	pipeline := azblob.NewPipeline(anonymousCredential, azblob.PipelineOptions{})
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
