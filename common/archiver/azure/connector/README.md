# Azure Blob Storage Connector

This package provides an abstraction layer between Temporal's archiver system and Azure Blob Storage, enabling seamless integration for archiving workflow history and visibility data.

## Overview

The connector implements a wrapper around the Azure Blob Storage Go SDK, providing standardized operations for uploading, downloading, querying, and managing blobs within Azure containers.

## Architecture

### Core Components

- **Client Interface** (`client.go`): Main interface defining storage operations
- **Client Delegate** (`client_delegate.go`): Azure SDK wrapper with authentication handling
- **Storage Wrapper**: Implementation of the Client interface

### Key Files

- `client.go` - Primary client interface and implementation
- `client_delegate.go` - Azure SDK abstraction layer with authentication
- `client_delegate_mock.go` - Generated mocks for testing
- `client_mock.go` - Generated mocks for the main client interface
- `client_test.go` - Unit tests

## Authentication

The connector supports multiple authentication methods:

1. **Connection String** - via `AZURE_STORAGE_CONNECTION_STRING` environment variable
2. **Account Credentials** - via `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` environment variables
3. **Anonymous Access** - for development/testing (limited functionality)

## Operations

### Upload
Uploads byte data to Azure Blob Storage with specified URI and filename.

### Download (Get)
Retrieves blob content as byte array.

### Query
Lists blobs matching a prefix, with optional filtering and pagination support.

### Existence Checks
Verifies if containers or specific blobs exist.

## Error Handling

- `ErrBucketNotFound` - Container does not exist
- `errObjectNotFound` - Blob does not exist
- Azure SDK errors are wrapped and propagated appropriately

## Usage

The connector integrates with Temporal's archiver system through the standard archiver.URI format:
```
as://container-name/path/to/archive/location
```

## Testing

Mock implementations are provided for unit testing, allowing the archiver logic to be tested independently of Azure Blob Storage connectivity.
