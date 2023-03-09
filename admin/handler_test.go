// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

//go:build integration

package admin

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestIntegration(t *testing.T) {
	stackID := "integration-test"
	topic := "test-topic"
	cluster := os.Getenv("MSK_CLUSTER")
	if cluster == "" {
		panic("Missing environment variable: MSK_CLUSTER")
	}
	arn := os.Getenv("IAM_ARN")
	if arn == "" {
		panic("Missing environment variable: IAM_ARN")
	}

	pid := runCreateHandler(t, stackID, defaultProperties(cluster, topic, arn))
	pid = runCreateHandler(t, stackID, defaultProperties(cluster, topic, arn))
	runUpdateHandler(t, stackID, pid, defaultProperties(cluster, topic, arn), buildPropertiesWith(cluster, topic, map[string]interface{}{
		"Users": []map[string]interface{}{},
		"Config": map[string]string{
			"cleanup.policy": "[delete, compact]",
		},
	}))
	runDeleteHandler(t, stackID, pid, defaultProperties(cluster, topic, arn))
	runDeleteHandler(t, stackID, pid, defaultProperties(cluster, topic, arn))
}

func runCreateHandler(t *testing.T, stackID string, properties map[string]interface{}) string {
	return runHandler(t, cfn.RequestCreate, stackID, "", properties, nil)
}

func runUpdateHandler(t *testing.T, stackID, physicalResourceID string, properties, oldProperties map[string]interface{}) {
	runHandler(t, cfn.RequestUpdate, stackID, physicalResourceID, properties, oldProperties)
}

func runDeleteHandler(t *testing.T, stackID, physicalResourceID string, properties map[string]interface{}) {
	runHandler(t, cfn.RequestDelete, stackID, physicalResourceID, properties, nil)
}

func runHandler(t *testing.T, requestType cfn.RequestType, stackID, physicalResourceID string, properties, oldProperties map[string]interface{}) string {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	mskClient := kafka.NewFromConfig(cfg)
	secretsManagerClient := secretsmanager.NewFromConfig(cfg)
	kmsClient := kms.NewFromConfig(cfg)
	kafkaClientProvider := NewIamKafkaClientProvider(mskClient)
	handler := NewHandler(mskClient, kmsClient, secretsManagerClient, kafkaClientProvider)
	rid, d, err := handler.Handle(ctx, cfn.Event{
		PhysicalResourceID:    physicalResourceID,
		RequestType:           requestType,
		StackID:               stackID,
		ResourceProperties:    properties,
		OldResourceProperties: oldProperties,
	})
	assert.NotEmpty(t, rid)
	if physicalResourceID != "" {
		assert.Equal(t, physicalResourceID, rid)
	}
	if requestType == cfn.RequestCreate {
		assert.NotEmpty(t, d[PropUsernameSuffix])
	}
	assert.Nil(t, err)
	logger, _ := zap.NewDevelopment()
	logger.Sugar().Infow("Handler Completed", "Operation", requestType, "PhysicalResourceID", rid)
	return rid
}

func defaultProperties(cluster, topic, arn string) map[string]interface{} {
	return buildPropertiesWith(cluster, topic, map[string]interface{}{
		"Users": []map[string]interface{}{
			{
				"Username":    "test-alice",
				"Arn":         arn,
				"Permissions": []string{"READ"},
			},
		},
	})
}

func buildPropertiesWith(cluster, topic string, props map[string]interface{}) map[string]interface{} {
	p := map[string]interface{}{
		"ServiceToken":      "token",
		"ClusterArn":        cluster,
		"Name":              topic,
		"Partitions":        "1",
		"ReplicationFactor": "3",
	}
	for k, v := range props {
		p[k] = v
	}
	return p
}
