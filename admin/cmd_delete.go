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

type cmdDelete struct {
	kmsKeyResolver KmsKeyResolverService
	userManager    UserManagerService
	kafkaClient    KafkaClient
	logger         *zap.Logger
}

func newCmdDelete(kmsKeyResolver KmsKeyResolverService, userManager UserManagerService, kafkaClient KafkaClient, logger *zap.Logger) *cmdDelete {
	return &cmdDelete{
		kmsKeyResolver: kmsKeyResolver,
		userManager:    userManager,
		kafkaClient:    kafkaClient,
		logger:         logger,
	}
}

func (a *cmdDelete) Run(ctx context.Context, info *types.TopicInfo, stackID string) error {
	kmsKeyID, err := a.kmsKeyResolver.Resolve(ctx, info)
	if err != nil {
		return errors.WithStack(err)
	}
	shortStackID := shortStackID(stackID)
	resourceID := canonicalTopicName(info.Name, shortStackID)
	if info.Users != nil {
		for _, u := range info.Users {
			err := a.userManager.DeleteUser(ctx, &u, kmsKeyID, resourceID, shortStackID, info.ClusterArn)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	if info.DeletionPolicy == types.DeletionPolicyRetain {
		a.logger.Sugar().Infow("Topic data not deleted due to deletion policy", "TopicName", resourceID)
		return nil
	}

	a.logger.Sugar().Infow("Start Operation", "Name", "DeleteTopics", "TopicName", resourceID)
	responses, err := a.kafkaClient.DeleteTopics(ctx, resourceID)
	if err != nil {
		return errors.WithStack(err)
	}
	a.logger.Sugar().Infow("DeleteTopics response", "Length", len(responses))
	if res, ok := responses[resourceID]; ok {
		if res.Err != nil {
			if !errors.Is(res.Err, kerr.UnknownTopicOrPartition) {
				return errors.WithStack(res.Err)
			}
			a.logger.Sugar().Infow("Retry Handled", "Operation", "DeleteTopics")
		}
	}
	return nil
}
