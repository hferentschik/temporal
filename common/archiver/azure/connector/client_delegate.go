//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_delegate_mock.go

package connector

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
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
		client *azblob.Client
	}
)

type (
	// ContainerHandleWrapper is an interface that exposes methods from Azure Blob Storage container
	ContainerHandleWrapper interface {
		Blob(blobName string) BlobHandleWrapper
		GetProperties(ctx context.Context) error
	}

	containerDelegate struct {
		client        *azblob.Client
		containerName string
	}
)

type (
	// BlobHandleWrapper is an interface that exposes methods from Azure Blob Storage blob
	BlobHandleWrapper interface {
		Upload(ctx context.Context, body io.ReadSeeker) error
		Download(ctx context.Context, writer io.Writer) error
		GetProperties(ctx context.Context) error
	}

	blobDelegate struct {
		client        *azblob.Client
		containerName string
		blobName      string
	}
)

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
		accountName, _ = os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
	}
	if tenantID == "" {
		tenantID, _ = os.LookupEnv("AZURE_TENANT_ID")
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

// newClientDelegateWithConfig creates a new Azure client similar to temporal-large-payload-codec pattern
func newClientDelegateWithConfig(ctx context.Context, config *Config) (*clientDelegate, error) {

	log.Printf("newClientDelegateWithConfig::Creating Azure Blob Storage client with account name: %s and tenant: %s", config.AccountName, config.TenantID)
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
	// Use azidentity.NewDefaultAzureCredential() like temporal-large-payload-codec
	log.Printf("newClientDelegateWithConfig::Creating Azure credential with tenant ID: %s", config.TenantID)
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		TenantID: config.TenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create azure credential: %w", err)
	}

	log.Printf("newClientDelegateWithConfig::Creating Azure Blob Storage client for service URL: %s", serviceURL)
	// Create client using new Azure SDK like temporal-large-payload-codec
	client, err := azblob.NewClient(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create azure client: %w", err)
	}

	return &clientDelegate{client: client}, nil
}

// Container returns a ContainerHandleWrapper, which provides operations on the named container.
// This call does not perform any network operations.
func (c *clientDelegate) Container(containerName string) ContainerHandleWrapper {
	return &containerDelegate{client: c.client, containerName: containerName}
}

// Blob returns a BlobHandleWrapper, which provides operations on the named blob. (does not perform network operations)
func (cd *containerDelegate) Blob(blobName string) BlobHandleWrapper {
	return &blobDelegate{client: cd.client, containerName: cd.containerName, blobName: blobName}
}

// GetProperties returns the container's properties
func (cd *containerDelegate) GetProperties(ctx context.Context) error {
	_, err := cd.client.ServiceClient().NewContainerClient(cd.containerName).GetProperties(ctx, nil)
	return err
}

// Upload uploads content to the blob
func (bd *blobDelegate) Upload(ctx context.Context, body io.ReadSeeker) error {
	_, err := bd.client.UploadStream(ctx, bd.containerName, bd.blobName, body, nil)
	return err
}

// Download downloads blob content
func (bd *blobDelegate) Download(ctx context.Context, writer io.Writer) error {
	resp, err := bd.client.DownloadStream(ctx, bd.containerName, bd.blobName, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return err
		}
		return err
	}
	_, err = io.Copy(writer, resp.Body)
	return err
}

// GetProperties returns the blob's properties
func (bd *blobDelegate) GetProperties(ctx context.Context) error {
	_, err := bd.client.ServiceClient().NewContainerClient(bd.containerName).NewBlobClient(bd.blobName).GetProperties(ctx, nil)
	return err
}
