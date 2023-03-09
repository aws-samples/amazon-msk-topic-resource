// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"go.uber.org/zap"
)

type IamKafkaClientProvider struct {
	mskClient MskClient
}

func (p *IamKafkaClientProvider) NewKafkaClient(ctx context.Context, clusterArn string) (KafkaClient, error) {
	logger := ctx.Value(contextKeyLogger).(*zap.Logger)
	b, err := p.mskClient.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{ClusterArn: &clusterArn})
	if err != nil {
		return nil, err
	}
	if b.BootstrapBrokerStringSaslIam == nil {
		return nil, errors.WithStack(errors.New("MSK cluster does not have IAM authentication enabled. IAM authentication must be enabled before managing topics using this CloudFormation custom resource."))
	}
	logger.Sugar().Infow("Operation Finished", "Name", "GetBootstrapBrokers", "BootstrapBrokerStringSaslIam", *b.BootstrapBrokerStringSaslIam)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*b.BootstrapBrokerStringSaslIam, ",")...),
		kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return aws.Auth{}, errors.WithStack(err)
			}
			creds, err := cfg.Credentials.Retrieve(ctx)
			if err != nil {
				return aws.Auth{}, errors.WithStack(err)
			}
			return aws.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
			}, nil
		})),
		kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}).DialContext),
		kgo.MaxVersions(kversion.V2_4_0()),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return kadm.NewClient(cl), nil
}

func NewIamKafkaClientProvider(mskClient MskClient) *IamKafkaClientProvider {
	return &IamKafkaClientProvider{mskClient}
}
