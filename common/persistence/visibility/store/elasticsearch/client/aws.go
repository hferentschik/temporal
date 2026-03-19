package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	v1credentials "github.com/aws/aws-sdk-go/aws/credentials"
	elasticaws "github.com/olivere/elastic/v7/aws/v4"
)

// v2ToV1CredentialsProvider adapts an AWS SDK v2 CredentialsProvider to the v1 Provider interface
// required by olivere/elastic's V4 signing client.
type v2ToV1CredentialsProvider struct {
	provider aws.CredentialsProvider
	ctx      context.Context
}

func (p *v2ToV1CredentialsProvider) Retrieve() (v1credentials.Value, error) {
	creds, err := p.provider.Retrieve(p.ctx)
	if err != nil {
		return v1credentials.Value{}, err
	}
	return v1credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		ProviderName:    creds.Source,
	}, nil
}

func (p *v2ToV1CredentialsProvider) IsExpired() bool {
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
		credentialsProvider = credentials.NewStaticCredentialsProvider(
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

	v1Creds := v1credentials.NewCredentials(&v2ToV1CredentialsProvider{
		provider: credentialsProvider,
		ctx:      ctx,
	})
	return elasticaws.NewV4SigningClient(v1Creds, config.Region), nil
}
