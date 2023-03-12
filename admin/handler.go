// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/aws-samples/amazon-msk-topic-resource/types"

	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type contextKey string

type KafkaClientProvider interface {
	NewKafkaClient(ctx context.Context, clusterArn string) (KafkaClient, error)
}

const (
	PropUsernameSuffix string = "UsernameSuffix"
)

var contextKeyLogger contextKey = contextKey("Logger")

// Handler implements CloudFormation custom resource extension interface.
type Handler struct {
	mskClient            MskClient
	kmsClient            KmsClient
	secretsManagerClient SecretsManagerClient
	kafkaClientProvider  KafkaClientProvider
}

// Entrypoint for handling custom resource managed by this extension.
func (h *Handler) Handle(ctx context.Context, event cfn.Event) (string, map[string]interface{}, error) {
	var physicalResourceID string
	var props map[string]interface{}
	var err error

	// Prepare logger with contextual information
	logger := h.initializeLogger(&event)
	ctx = context.WithValue(ctx, contextKeyLogger, logger)
	defer logger.Sync()
	logger.Info("Start", zap.Any("ResourceProperties", event.ResourceProperties), zap.Any("OldResourceProperties", event.OldResourceProperties))

	switch event.RequestType {
	case cfn.RequestCreate:
		physicalResourceID, props, err = h.create(ctx, event, logger)
	case cfn.RequestUpdate:
		physicalResourceID, props, err = h.update(ctx, event, logger)
	case cfn.RequestDelete:
		physicalResourceID, props, err = h.delete(ctx, event, logger)
	default:
		err = fmt.Errorf("unknown request type: %v", event.RequestType)
	}
	return physicalResourceID, props, h.logAndEchoError(event, err, logger)
}

func (h *Handler) logAndEchoError(event cfn.Event, err error, logger *zap.Logger) error {
	if err == nil {
		return err
	}
	logger.Error("Failed to process request", zap.Error(err))
	// Log more information if this is an error cause by an AWS SDK operation
	var oerr *smithy.OperationError
	if errors.As(err, &oerr) {
		if oerr.Unwrap() != nil {
			logger.Error("Smithy Operation Error", zap.Error(oerr))
		}
	}
	return err
}

func (h *Handler) create(ctx context.Context, event cfn.Event, logger *zap.Logger) (string, map[string]interface{}, error) {
	props := make(map[string]interface{})
	// CFN requires PhysicalResourceID even for error results.
	// There's a bug in aws-lambda-go package that does not handle this scenario correctly.
	// https://github.com/aws/aws-lambda-go/issues/107
	// Initialise a UUID to be used as PhysicalResourceID on error paths as a workaround.
	rid := uuid.NewString()
	ti, err := types.NewTopicInfo(event.ResourceProperties)
	if err != nil {
		return rid, nil, err
	}
	kafkaClient, err := h.kafkaClientProvider.NewKafkaClient(ctx, ti.ClusterArn)
	if err != nil {
		return rid, nil, err
	}
	userManager := newUserManager(h.secretsManagerClient, h.kmsClient, h.mskClient, kafkaClient, logger, func() { time.Sleep(time.Second * 30) })
	kmsKeyResolver := newKmsKeyResolver(h.mskClient)
	cmdCreate := newCmdCreate(kafkaClient, kmsKeyResolver, userManager, logger)
	id, err := cmdCreate.Run(ctx, ti, event.StackID)
	if err == nil {
		rid = id.PhysicalResourceID
		props[PropUsernameSuffix] = id.UsernameSuffix
	}
	return rid, props, err
}

func (h *Handler) update(ctx context.Context, event cfn.Event, logger *zap.Logger) (string, map[string]interface{}, error) {
	old, err := types.NewTopicInfo(event.OldResourceProperties)
	if err != nil {
		return event.PhysicalResourceID, nil, errors.WithStack(err)
	}
	new, err := types.NewTopicInfo(event.ResourceProperties)
	if err != nil {
		return event.PhysicalResourceID, nil, errors.WithStack(err)
	}
	kafkaClient, err := h.kafkaClientProvider.NewKafkaClient(ctx, old.ClusterArn)
	if err != nil {
		return event.PhysicalResourceID, nil, err
	}
	userManager := newUserManager(h.secretsManagerClient, h.kmsClient, h.mskClient, kafkaClient, logger, func() { time.Sleep(time.Second * 30) })
	kmsKeyResolver := newKmsKeyResolver(h.mskClient)
	cmdUpdate := newCmdUpdate(kmsKeyResolver, userManager, kafkaClient, func() { time.Sleep(time.Second * 30) }, logger)
	err = cmdUpdate.Run(ctx, old, new, event.StackID)
	return event.PhysicalResourceID, nil, err
}

func (h *Handler) delete(ctx context.Context, event cfn.Event, logger *zap.Logger) (string, map[string]interface{}, error) {
	ti, err := types.NewTopicInfo(event.ResourceProperties)
	if err != nil {
		return event.PhysicalResourceID, nil, err
	}
	kafkaClient, err := h.kafkaClientProvider.NewKafkaClient(ctx, ti.ClusterArn)
	if err != nil {
		return event.PhysicalResourceID, nil, err
	}
	userManager := newUserManager(h.secretsManagerClient, h.kmsClient, h.mskClient, kafkaClient, logger, func() { time.Sleep(time.Second * 30) })
	kmsKeyResolver := newKmsKeyResolver(h.mskClient)
	cmdDelete := newCmdDelete(kmsKeyResolver, userManager, kafkaClient, logger)
	err = cmdDelete.Run(ctx, ti, event.StackID)
	return event.PhysicalResourceID, nil, err
}

func (h *Handler) initializeLogger(event *cfn.Event) *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	return logger.With(
		zap.String("StackID", event.StackID),
		zap.String("LogicalResourceID", event.LogicalResourceID),
		zap.String("PhysicalResourceID", event.PhysicalResourceID),
		zap.String("RequestID", event.RequestID),
		zap.String("ResourceType", event.ResourceType),
		zap.String("RequestType", string(event.RequestType)),
	)
}

func NewHandler(mskClient MskClient, kmsClient KmsClient, secretsManagerClient SecretsManagerClient, kafkaClientProvider KafkaClientProvider) *Handler {
	return &Handler{
		mskClient:            mskClient,
		kmsClient:            kmsClient,
		secretsManagerClient: secretsManagerClient,
		kafkaClientProvider:  kafkaClientProvider,
	}
}
