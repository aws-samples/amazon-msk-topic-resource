// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"testing"
	"tr/admin/mocks"
	tt "tr/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

func TestCmdUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	type testCase struct {
		name                       string
		topic                      string
		old                        *tt.TopicInfo
		new                        *tt.TopicInfo
		expectedUserDiff           *userDiff
		err                        error
		describeTopicConfigsOutput []interface{}
		kmsResolverOutput          []interface{}
		createUserOutput           map[string][]interface{}
		deleteUserOutput           map[string][]interface{}
		createACLsOutput           map[string][]interface{}
		deleteACLsOutput           map[string][]interface{}
		deletedConfigProps         map[string]*string
		addedConfigProps           map[string]*string
		updatedConfigProps         map[string]*string
		listTopicsOutput           []interface{}
	}

	alice := tt.User{Username: "alice", Arn: "1", Permissions: []tt.Permission{tt.PermissionRead}}
	bob := tt.User{Username: "bob", Arn: "2", Permissions: []tt.Permission{tt.PermissionRead}}
	bobRW := tt.User{Username: "bob", Arn: "2", Permissions: []tt.Permission{tt.PermissionRead, tt.PermissionWrite}}
	bobW := tt.User{Username: "bob", Arn: "2", Permissions: []tt.Permission{tt.PermissionWrite}}
	bobArn3 := tt.User{Username: "bob", Arn: "3", Permissions: []tt.Permission{tt.PermissionRead}}
	aliceNoArn := tt.User{Username: "alice", Permissions: []tt.Permission{tt.PermissionRead}}

	configValue1 := aws.String("1")
	configValue2 := aws.String("2")
	configValue3 := aws.String("3")
	configValue4 := aws.String("1")

	cases := []testCase{
		{
			name:             "Empty users list",
			topic:            "a",
			old:              &tt.TopicInfo{Name: "a"},
			new:              &tt.TopicInfo{Name: "a"},
			expectedUserDiff: newUserDiff(),
		},
		{
			name:             "Identical users list",
			topic:            "a",
			old:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice, bob}},
			new:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice, bob}},
			expectedUserDiff: newUserDiff(),
		},
		{
			name:             "Added user",
			topic:            "a",
			old:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice}},
			new:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice, bob}},
			expectedUserDiff: newUserDiff(withAddedUsers([]*tt.User{&bob})),
		},
		{
			name:             "Added permission",
			topic:            "a",
			old:              &tt.TopicInfo{Name: "a", Users: []tt.User{bob}},
			new:              &tt.TopicInfo{Name: "a", Users: []tt.User{bobRW}},
			expectedUserDiff: newUserDiff(withAddedPermissions("bob", []tt.Permission{tt.PermissionWrite})),
		},
		{
			name:  "Replaced permission",
			topic: "a",
			old:   &tt.TopicInfo{Name: "a", Users: []tt.User{bob}},
			new:   &tt.TopicInfo{Name: "a", Users: []tt.User{bobW}},
			expectedUserDiff: newUserDiff(
				withAddedPermissions("bob", []tt.Permission{tt.PermissionWrite}),
				withDeletedPermissions("bob", []tt.Permission{tt.PermissionRead}),
			),
		},
		{
			name:  "Updated arn",
			topic: "a",
			old:   &tt.TopicInfo{Name: "a", Users: []tt.User{bob}},
			new:   &tt.TopicInfo{Name: "a", Users: []tt.User{bobArn3}},
			expectedUserDiff: newUserDiff(
				withAddedUsers([]*tt.User{&bobArn3}),
				withDeletedUsers([]*tt.User{&bob}),
			),
		},
		{
			name:             "Deleted user",
			topic:            "a",
			old:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice, bob}},
			new:              &tt.TopicInfo{Name: "a", Users: []tt.User{alice}},
			expectedUserDiff: newUserDiff(withDeletedUsers([]*tt.User{&bob})),
		},
		{
			name:  "Deleted user and update another user's permissions",
			topic: "a",
			old:   &tt.TopicInfo{Name: "a", Users: []tt.User{alice, bob}},
			new:   &tt.TopicInfo{Name: "a", Users: []tt.User{bobRW}},
			expectedUserDiff: newUserDiff(
				withAddedPermissions("bob", []tt.Permission{tt.PermissionWrite}),
				withDeletedUsers([]*tt.User{&alice}),
			),
		},
		{
			name:  "Deleted user without an arn",
			topic: "a",
			old:   &tt.TopicInfo{Name: "a", Users: []tt.User{aliceNoArn}},
			new:   &tt.TopicInfo{Name: "a", Users: []tt.User{}},
			expectedUserDiff: newUserDiff(
				withDeletedUsers([]*tt.User{&aliceNoArn}),
			),
		},
		{
			name:                       "Config updates",
			topic:                      "a",
			old:                        &tt.TopicInfo{Name: "a", Users: []tt.User{aliceNoArn}, Config: map[string]*string{"a": configValue1, "b": configValue2, "c": configValue1}},
			new:                        &tt.TopicInfo{Name: "a", Users: []tt.User{aliceNoArn}, Config: map[string]*string{"a": configValue2, "c": configValue1, "d": configValue3}},
			describeTopicConfigsOutput: []interface{}{kadm.ResourceConfigs{kadm.ResourceConfig{Name: "a", Configs: []kadm.Config{{Key: "a", Value: configValue4}, {Key: "b", Value: configValue2}, {Key: "c", Value: configValue4}, {Key: "x", Value: configValue1}}}}, error(nil)},
			addedConfigProps:           map[string]*string{"d": configValue3},
			updatedConfigProps:         map[string]*string{"a": configValue2},
			deletedConfigProps:         map[string]*string{"b": configValue2},
		},
	}

	stackID := "test"
	kmsKeyID := "test-kms-key-arn"
	shortStackID := shortStackID(stackID)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange
			ctx := context.TODO()
			kmsKeyID = uuid.NewString()
			topicName := canonicalTopicName(c.new.Name, shortStackID)

			if c.describeTopicConfigsOutput == nil {
				c.describeTopicConfigsOutput = []interface{}{kadm.ResourceConfigs{kadm.ResourceConfig{}}, error(nil)}
			}
			if c.kmsResolverOutput == nil {
				c.kmsResolverOutput = []interface{}{kmsKeyID, error(nil)}
			}
			if c.createUserOutput == nil {
				c.createUserOutput = make(map[string][]interface{})
			}
			if c.deleteUserOutput == nil {
				c.deleteUserOutput = make(map[string][]interface{})
			}
			if c.createACLsOutput == nil {
				c.createACLsOutput = make(map[string][]interface{})
			}
			if c.deleteACLsOutput == nil {
				c.deleteACLsOutput = make(map[string][]interface{})
			}
			if c.expectedUserDiff == nil {
				c.expectedUserDiff = &userDiff{}
			}
			if c.addedConfigProps == nil {
				c.addedConfigProps = make(map[string]*string)
			}
			if c.updatedConfigProps == nil {
				c.updatedConfigProps = make(map[string]*string)
			}
			if c.deletedConfigProps == nil {
				c.deletedConfigProps = make(map[string]*string)
			}
			if c.listTopicsOutput == nil {
				pd := make(kadm.PartitionDetails)
				for p := int32(0); p < int32(c.new.Partitions); p++ {
					replicas := make([]int32, c.new.ReplicationFactor)
					pd[p] = kadm.PartitionDetail{Topic: topicName, Partition: p, Replicas: replicas}
				}
				c.listTopicsOutput = []interface{}{kadm.TopicDetails{topicName: kadm.TopicDetail{Topic: topicName, Partitions: pd}}, error(nil)}
			}

			kafkaClient := mocks.NewMockKafkaClient(ctrl)
			kmsKeyResolver := mocks.NewMockKmsKeyResolverService(ctrl)
			userManager := mocks.NewMockUserManagerService(ctrl)

			cmdUpdate := newCmdUpdate(kmsKeyResolver, userManager, kafkaClient, func() {}, logger)

			kafkaClient.EXPECT().ListTopics(ctx, topicName).Return(c.listTopicsOutput...)
			kafkaClient.EXPECT().DescribeTopicConfigs(ctx, topicName).Return(c.describeTopicConfigsOutput...)
			kmsKeyResolver.EXPECT().Resolve(ctx, c.new).Return(c.kmsResolverOutput...)

			if len(c.addedConfigProps) > 0 || len(c.updatedConfigProps) > 0 || len(c.deletedConfigProps) > 0 {
				kafkaClient.EXPECT().AlterTopicConfigs(ctx, gomock.Any(), topicName).
					Do(func(actx context.Context, alts []kadm.AlterConfig, tn string) {
						assert.Equal(t, ctx, actx)
						assert.Equal(t, topicName, tn)
						for _, a := range alts {
							switch a.Op {
							case kadm.SetConfig:
								assert.Equal(t, c.updatedConfigProps[a.Name], a.Value)
							case kadm.AppendConfig:
								assert.Equal(t, c.addedConfigProps[a.Name], a.Value)
							case kadm.DeleteConfig:
								assert.Equal(t, c.deletedConfigProps[a.Name], a.Value)
							}
						}
					}).
					Return(kadm.AlterConfigsResponses{kadm.AlterConfigsResponse{Name: topicName}}, error(nil))
			}

			for _, a := range c.expectedUserDiff.DeletedUsers {
				if _, ok := c.deleteUserOutput[a.Username]; !ok {
					c.deleteUserOutput[a.Username] = []interface{}{error(nil)}
				}
				userManager.EXPECT().DeleteUser(ctx, a, kmsKeyID, topicName, shortStackID, c.old.ClusterArn).Return(c.deleteUserOutput[a.Username]...)
			}

			for _, a := range c.expectedUserDiff.AddedUsers {
				if _, ok := c.createUserOutput[a.Username]; !ok {
					c.createUserOutput[a.Username] = []interface{}{error(nil)}
				}
				userManager.EXPECT().CreateUser(ctx, shortStackID, topicName, kmsKeyID, c.old.ClusterArn, a).Return(c.createUserOutput[a.Username]...)
			}

			for u := range c.expectedUserDiff.AddedPermissions {
				if _, ok := c.createACLsOutput[u]; !ok {
					c.createACLsOutput[u] = []interface{}{error(nil)}
				}
				userManager.EXPECT().CreateACLs(ctx, topicName, u, shortStackID, c.expectedUserDiff.AddedPermissions[u])
			}

			for u := range c.expectedUserDiff.DeletedPermissions {
				if _, ok := c.deleteACLsOutput[u]; !ok {
					c.deleteACLsOutput[u] = []interface{}{error(nil)}
				}
				userManager.EXPECT().DeleteACLs(ctx, topicName, u, shortStackID, c.expectedUserDiff.DeletedPermissions[u])
			}

			// Act
			err := cmdUpdate.Run(ctx, c.old, c.new, stackID)

			// Assert
			assert.Equal(t, c.err, err)
		})
	}
}
