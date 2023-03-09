// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package types

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

const topicInfoSchema string = `
{
	"$id": "https://github.com/buddhike/msktr/topic-info-schema.json",
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"description": "MSK Topic schema used for managing MSK topics via MSK Topic Resource custom CloudFormation resource.",
	"type": "object",
	"required": ["ServiceToken", "Name", "Partitions", "ReplicationFactor", "ClusterArn"],
	"definitions": {
		"User": {
			"type": "object",
			"required": ["Username", "Permissions"],
			"description": "User with access to MSK topic",
			"properties": {
				"Username": {
					"type": "string",
					"description": "Username for the user. TR will append a short random string to ensure that usernames created via different stacks do not conflict. All usernames created within a stack has the same suffix."
				},
				"Arn": {
					"type": "string",
					"description": "ARN of an IAM entity that should have access to the SecretsManager secret containing credentials for the user. This can be used to configure producers and consumers discover credentials at runtime."
				},
				"Permissions": { 
					"type": "array",
					"description": "Operations allowed for this user. Available options are READ/WRITE.",
					"items": {
						"type": "string",
						"enum": [
							"READ",
							"WRITE"
						]
					}
				} 
			},
			"additionalProperties": false
		}
	},
	"properties": {
		"ServiceToken": {
			"type": "string",
			"description": "ARN of TR custom resource Lambda function. This can be found in the output of CloudFormation stack used to install TR in your account."
		},
		"Name": {
			"type": "string",
			"description": "Topic name. TR will append a short random string to ensure that topic names created via different stacks do not conflict. All topics created within a stack has the same suffix."
		},
		"Partitions": {
			"type": "string",
			"description": "Number of partitions in this topic",
			"pattern": "^[0-9]*$"
		},
		"ReplicationFactor": {
			"type": "string",
			"description": "Replication factor for the topic",
			"pattern": "^[0-9]*$"
		},
		"ClusterArn": {
			"description": "MSK cluster ARN",
			"type": "string"
		},
		"Config": {
			"type": "object",
			"properties": {},
			"description": "Additional topic configuration properties. Any Kafka topic property such as min.insync.replicas or MSK specific topic property such local.retention.ms can be specified here.",
			"additionalProperties": true
		},
		"Users": {
			"type": "array",
			"description": "List of users and their permissions",
			"items": { "$ref": "#/definitions/User" }
		},
		"DeletionPolicy": {
			"type": "string",
			"description": "Specify what to be done to the topic and data when the CloudFormation stack is deleted",
			"enum": ["DELETE", "RETAIN"]
		}
	},
	"additionalProperties": false
}
`

type Permission string
type DeletionPolicy string

const (
	PermissionRead       Permission     = "READ"
	PermissionWrite      Permission     = "WRITE"
	DeletionPolicyDelete DeletionPolicy = "DELETE"
	DeletionPolicyRetain DeletionPolicy = "RETAIN"
)

type User struct {
	Username    string
	Arn         string
	Permissions []Permission
}

type TopicInfo struct {
	Name              string
	Partitions        int `json:",string"`
	ReplicationFactor int `json:",string"`
	ClusterArn        string
	Config            map[string]*string
	Users             []User
	DeletionPolicy    DeletionPolicy
}

func NewTopicInfo(props map[string]interface{}) (*TopicInfo, error) {
	buf, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	schemaLoader := gojsonschema.NewStringLoader(topicInfoSchema)
	documentLoader := gojsonschema.NewBytesLoader(buf)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return nil, err
	}

	if result.Valid() {
		var ti = TopicInfo{DeletionPolicy: DeletionPolicyRetain}
		err := json.Unmarshal(buf, &ti)
		return &ti, err
	} else {
		msgs := make([]string, len(result.Errors()))
		for i, e := range result.Errors() {
			msgs[i] = e.String()
		}
		return nil, errors.New(strings.Join(msgs, " "))
	}
}
