//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_delegate_mock.go

package connector

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type (
	// AzureBlobStorageClient is an interface that exposes methods from Azure Blob Storage client
	AzureBlobStorageClient interface {
		Container(containerName string) ContainerHandleWrapper
	}

	// Config holds the configuration for Azure Blob Storage client (similar to temporal-large-payload-codec)
	Config struct {
		AccountName string
		TenantID    string
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
	return newDefaultClientDelegateWithConfig(ctx, nil)
}

// newDefaultClientDelegateWithConfig creates a new Azure Blob Storage client using config with environment variable fallback
func newDefaultClientDelegateWithConfig(ctx context.Context, config *Config) (*clientDelegate, error) {
	var accountName, tenantID string

	// 1. First priority: Use config if provided
	if config != nil {
		accountName = config.AccountName
		tenantID = config.TenantID
	}

	// 2. Second priority: Environment variables as fallback
	if accountName == "" {
		accountName = os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	}
	if tenantID == "" {
		tenantID = os.Getenv("AZURE_TENANT_ID")
	}

	// 3. Validate that we have the required values
	if accountName == "" {
		return nil, fmt.Errorf("Azure storage account name is required - provide via config or AZURE_STORAGE_ACCOUNT_NAME environment variable")
	}
	if tenantID == "" {
		return nil, fmt.Errorf("Azure tenant ID is required - provide via config or AZURE_TENANT_ID environment variable")
	}

	finalConfig := &Config{
		AccountName: accountName,
		TenantID:    tenantID,
	}

	return newClientDelegateWithConfig(ctx, finalConfig)
}

// newClientDelegateWithManagedIdentity creates a new Azure Blob Storage client using managed identity
func newClientDelegateWithManagedIdentity(ctx context.Context, accountName, tenantID string) (*clientDelegate, error) {
	config := &Config{
		AccountName: accountName,
		TenantID:    tenantID,
	}
	return newClientDelegateWithConfig(ctx, config)
}

// newClientDelegateWithConfig creates a new Azure client similar to temporal-large-payload-codec pattern
func newClientDelegateWithConfig(ctx context.Context, config *Config) (*clientDelegate, error) {
	serviceURL, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", config.AccountName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse storage account URL: %w", err)
	}

	// Use azidentity.NewDefaultAzureCredential() like temporal-large-payload-codec
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to create azure credential: %w", err)
	}

	// Create a token credential that works with the old azblob SDK
	// This bridges the new azidentity with the old azblob SDK
	tokenCredential := azblob.NewTokenCredential("", func(credential azblob.TokenCredential) time.Duration {
		// Get a new token from azidentity
		token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{
			Scopes: []string{"https://storage.azure.com/.default"},
		})
		if err != nil {
			// Return 0 to stop refreshing on error
			return 0
		}

		// Update the token credential
		credential.SetToken(token.Token)

		// Calculate refresh duration (refresh 5 minutes before expiry)
		refreshIn := time.Until(token.ExpiresOn) - (5 * time.Minute)
		if refreshIn <= 0 {
			refreshIn = 1 * time.Minute // Minimum refresh interval
		}
		return refreshIn
	})

	credential := tokenCredential
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
