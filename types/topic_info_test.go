// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTopicInfo(t *testing.T) {
	type testCase struct {
		Input  map[string]interface{}
		Output *TopicInfo
		Err    error
	}
	cases := map[string]testCase{
		"Empty map": {
			Input: map[string]interface{}{},
			Err:   errors.New("(root): ServiceToken is required (root): Name is required (root): Partitions is required (root): ReplicationFactor is required (root): ClusterArn is required"),
		},
		"Basic Topic Info": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
			},
			Output: &TopicInfo{
				Name:              "topic-a",
				Partitions:        1,
				ReplicationFactor: 3,
				ClusterArn:        "arn",
				DeletionPolicy:    DeletionPolicyRetain,
			},
		},
		"Topic with users": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
				"Users": []map[string]interface{}{
					{"Username": "alice", "Arn": "a", "Permissions": []string{"READ"}},
					{"Username": "bob", "Arn": "b", "Permissions": []string{"READ", "WRITE"}},
				},
			},
			Output: &TopicInfo{
				Name:              "topic-a",
				Partitions:        1,
				ReplicationFactor: 3,
				ClusterArn:        "arn",
				Users: []User{
					{Username: "alice", Arn: "a", Permissions: []Permission{"READ"}},
					{Username: "bob", Arn: "b", Permissions: []Permission{"READ", "WRITE"}},
				},
				DeletionPolicy: DeletionPolicyRetain,
			},
		},
		"Invalid Partitions": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1a",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
				"Users": []map[string]interface{}{
					{"Username": "alice", "Arn": "a", "Permissions": []string{"READ"}},
				},
			},
			Err: errors.New("Partitions: Does not match pattern '^[0-9]*$'"),
		},
		"Invalid Replication Factor": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3a",
				"ClusterArn":        "arn",
				"Users": []map[string]interface{}{
					{"Username": "alice", "Arn": "a", "Permissions": []string{"READ"}},
				},
			},
			Err: errors.New("ReplicationFactor: Does not match pattern '^[0-9]*$'"),
		},
		"Invalid permission": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
				"Users": []map[string]interface{}{
					{"Username": "alice", "Arn": "a", "Permissions": []string{"READING"}},
				},
			},
			Err: errors.New("Users.0.Permissions.0: Users.0.Permissions.0 must be one of the following: \"READ\", \"WRITE\""),
		},
		"DeletionPolicyRetain": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
				"DeletionPolicy":    "RETAIN",
			},
			Output: &TopicInfo{
				Name:              "topic-a",
				Partitions:        1,
				ReplicationFactor: 3,
				ClusterArn:        "arn",
				DeletionPolicy:    DeletionPolicyRetain,
			},
		},
		"Invalid DeletionPolicy": {
			Input: map[string]interface{}{
				"ServiceToken":      "st",
				"Name":              "topic-a",
				"Partitions":        "1",
				"ReplicationFactor": "3",
				"ClusterArn":        "arn",
				"DeletionPolicy":    "INVALID_POLICY",
			},
			Err: errors.New("DeletionPolicy: DeletionPolicy must be one of the following: \"DELETE\", \"RETAIN\""),
		},
	}

	for k, c := range cases {
		ti, err := NewTopicInfo(c.Input)
		assert.Equal(t, c.Err, err, k)
		assert.Equal(t, c.Output, ti, k)
	}
}
