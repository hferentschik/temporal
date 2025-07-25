package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	sdkv1creds "github.com/aws/aws-sdk-go/aws/credentials"
	elasticaws "github.com/olivere/elastic/v7/aws/v4"
)

// v2CredentialsAdapter wraps an AWS SDK v2 CredentialsProvider so it can be
// used as an AWS SDK v1 credentials.Provider.  This lets us resolve credentials
// with SDK v2 while keeping the upstream olivere/elastic dependency unchanged
// (its NewV4SigningClient expects SDK v1 *credentials.Credentials).
type v2CredentialsAdapter struct {
	provider aws.CredentialsProvider
}

func (a *v2CredentialsAdapter) Retrieve() (sdkv1creds.Value, error) {
	creds, err := a.provider.Retrieve(context.Background())
	if err != nil {
		return sdkv1creds.Value{}, err
	}
	return sdkv1creds.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		ProviderName:    creds.Source,
	}, nil
}

func (a *v2CredentialsAdapter) IsExpired() bool {
	return false
}

func NewAwsHttpClient(config ESAWSRequestSigningConfig) (*http.Client, error) {
	if !config.Enabled {
		return nil, nil
	}

	if config.Region == "" {
		config.Region = os.Getenv("AWS_REGION")
		if config.Region == "" {
			return nil, fmt.Errorf("unable to resolve AWS region for obtaining AWS Elastic signing credentials")
		}
	}

	ctx := context.Background()
	var credentialsProvider aws.CredentialsProvider

	switch strings.ToLower(config.CredentialProvider) {
	case "static":
		credentialsProvider = awscreds.NewStaticCredentialsProvider(
			config.Static.AccessKeyID,
			config.Static.SecretAccessKey,
			config.Static.Token,
		)
	case "environment", "aws-sdk-default":
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(config.Region),
		)
		if err != nil {
			return nil, err
		}
		credentialsProvider = cfg.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'", config.CredentialProvider)
	}

	v1Creds := sdkv1creds.NewCredentials(&v2CredentialsAdapter{provider: credentialsProvider})
	return elasticaws.NewV4SigningClient(v1Creds, config.Region), nil
}
