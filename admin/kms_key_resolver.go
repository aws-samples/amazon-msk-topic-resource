// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"fmt"
	"tr/types"

	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/pkg/errors"
)

type KmsKeyResolverService interface {
	Resolve(ctx context.Context, info *types.TopicInfo) (string, error)
}

const TagKmsKey = "TR-KMS-KEY"

type kmsKeyResolver struct {
	mskClient MskClient
}

func newKmsKeyResolver(mskClient MskClient) *kmsKeyResolver {
	return &kmsKeyResolver{
		mskClient: mskClient,
	}
}

// KMS key used for encrypting SecretsManager secrets is expected to be created and
// managed along with MSK cluster. MSK cluster administrators should store KMS key
// ARN under a tag named TR-KMS-KEY in MSK cluster so that TR function can resolve
// it.
// If info has users and KMS key cannot be resolved as per above, this function
// returns an error.
func (a *kmsKeyResolver) Resolve(ctx context.Context, info *types.TopicInfo) (string, error) {
	var kmsKey string
	// If we have to setup users ensure that cluster has a kms key.
	if len(info.Users) > 0 {
		cluster, err := a.mskClient.DescribeCluster(ctx, &kafka.DescribeClusterInput{
			ClusterArn: &info.ClusterArn,
		})
		if err != nil {
			return "", errors.WithStack(err)
		}
		var ok bool
		if kmsKey, ok = cluster.ClusterInfo.Tags[TagKmsKey]; !ok {
			return "", errors.WithStack(fmt.Errorf("MSK cluster must have a tag named %s specifying the ARN of KMS key used for encrypting SASL/SCRAM credentials.", TagKmsKey))
		}
	}
	return kmsKey, nil
}
