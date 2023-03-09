// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"
	"tr/admin"

	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

var handler cfn.CustomResourceLambdaFunction

func init() {
	// Create the handler and store in global space to re-use
	// client instances between multiple invocations of Lambda.
	handler = cfn.LambdaWrap(func(ctx context.Context, event cfn.Event) (string, map[string]interface{}, error) {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", nil, err
		}
		mskClient := kafka.NewFromConfig(cfg)
		secretsManagerClient := secretsmanager.NewFromConfig(cfg)
		kmsClient := kms.NewFromConfig(cfg)
		kafkaClientProvider := admin.NewIamKafkaClientProvider(mskClient)
		handler := admin.NewHandler(mskClient, kmsClient, secretsManagerClient, kafkaClientProvider)
		return handler.Handle(ctx, event)
	})
}

func main() {
	lambda.Start(handler)
}
