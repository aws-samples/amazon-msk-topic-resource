// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/twmb/franz-go/pkg/kadm"
)

type KafkaClient interface {
	CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topic string) (kadm.CreateTopicResponse, error)
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error)
	DeleteTopics(ctx context.Context, topics ...string) (kadm.DeleteTopicResponses, error)
	CreateACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error)
	DescribeACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.DescribeACLsResults, error)
	DeleteACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.DeleteACLsResults, error)
}

type KmsClient interface {
	CreateGrant(ctx context.Context, params *kms.CreateGrantInput, optFns ...func(*kms.Options)) (*kms.CreateGrantOutput, error)
	RevokeGrant(ctx context.Context, params *kms.RevokeGrantInput, optFns ...func(*kms.Options)) (*kms.RevokeGrantOutput, error)
}

type SecretsManagerClient interface {
	DescribeSecret(ctx context.Context, params *secretsmanager.DescribeSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
	CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	DeleteSecret(ctx context.Context, params *secretsmanager.DeleteSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error)
	ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
	PutResourcePolicy(ctx context.Context, params *secretsmanager.PutResourcePolicyInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutResourcePolicyOutput, error)
}

type MskClient interface {
	GetBootstrapBrokers(ctx context.Context, params *kafka.GetBootstrapBrokersInput, optFns ...func(*kafka.Options)) (*kafka.GetBootstrapBrokersOutput, error)
	DescribeCluster(ctx context.Context, params *kafka.DescribeClusterInput, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterOutput, error)
	BatchAssociateScramSecret(ctx context.Context, params *kafka.BatchAssociateScramSecretInput, optFns ...func(*kafka.Options)) (*kafka.BatchAssociateScramSecretOutput, error)
	BatchDisassociateScramSecret(ctx context.Context, params *kafka.BatchDisassociateScramSecretInput, optFns ...func(*kafka.Options)) (*kafka.BatchDisassociateScramSecretOutput, error)
}
