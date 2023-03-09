// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"tr/types"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

type cmdCreate struct {
	kafkaClient    KafkaClient
	kmsKeyResolver KmsKeyResolverService
	userManager    UserManagerService
	logger         *zap.Logger
}

type createTopicResult struct {
	PhysicalResourceID string
	UsernameSuffix     string
}

func newCmdCreate(kafkaClient KafkaClient, kmsKeyResolver KmsKeyResolverService, userManager UserManagerService, logger *zap.Logger) *cmdCreate {
	return &cmdCreate{
		kafkaClient:    kafkaClient,
		kmsKeyResolver: kmsKeyResolver,
		userManager:    userManager,
		logger:         logger,
	}
}

func (a *cmdCreate) Run(ctx context.Context, info *types.TopicInfo, stackID string) (*createTopicResult, error) {
	kmsKeyID, err := a.kmsKeyResolver.Resolve(ctx, info)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	shortStackID := shortStackID(stackID)
	topicName := canonicalTopicName(info.Name, shortStackID)
	a.logger.Sugar().Infow("Start Operation", "Name", "CreateTopic", "TopicName", topicName)
	_, err = a.kafkaClient.CreateTopic(ctx, int32(info.Partitions), int16(info.ReplicationFactor), info.Config, topicName)
	if err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return nil, errors.WithStack(err)
		}
		a.logger.Sugar().Infow("Retry Handled", "Operation", "CreateTopic", "TopicName", topicName)
	}
	for _, u := range info.Users {
		err := a.userManager.CreateUser(ctx, shortStackID, topicName, kmsKeyID, info.ClusterArn, &u)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	a.logger.Sugar().Infow("Topic configuration successfully completed")
	return &createTopicResult{
		PhysicalResourceID: topicName,
		UsernameSuffix:     shortStackID,
	}, nil
}
