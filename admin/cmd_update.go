// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"tr/types"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

type cmdUpdate struct {
	kmsKeyResolver KmsKeyResolverService
	userManager    UserManagerService
	kafkaClient    KafkaClient
	fixedDelay     func()
	logger         *zap.Logger
}

func newCmdUpdate(kmsKeyResolver KmsKeyResolverService, userManager UserManagerService, kafkaClient KafkaClient, fixedDelay func(), logger *zap.Logger) *cmdUpdate {
	return &cmdUpdate{
		kmsKeyResolver: kmsKeyResolver,
		userManager:    userManager,
		kafkaClient:    kafkaClient,
		fixedDelay:     fixedDelay,
		logger:         logger,
	}
}

func (a *cmdUpdate) Run(ctx context.Context, old, new *types.TopicInfo, stackID string) error {
	shortStackID := shortStackID(stackID)
	topicName := canonicalTopicName(new.Name, shortStackID)
	topics, err := a.kafkaClient.ListTopics(ctx, topicName)
	if err != nil {
		return errors.WithStack(err)
	}
	if currentTopic, ok := topics[topicName]; ok && currentTopic.Err != nil {
		if errors.Is(currentTopic.Err, kerr.UnknownTopicOrPartition) && old.Name != new.Name {
			return errors.New("cannot update Name and ClusterArn properties")
		} else {
			return errors.WithStack(currentTopic.Err)
		}
	}
	currentTopic := topics[topicName]
	if len(currentTopic.Partitions.Numbers()) != new.Partitions {
		return errors.New("Cannot update Partitions")
	}
	if currentTopic.Partitions.NumReplicas() != new.ReplicationFactor {
		return errors.New("Cannot update ReplicationFactor")
	}

	kmsKeyID, err := a.kmsKeyResolver.Resolve(ctx, new)
	if err != nil {
		return errors.WithStack(err)
	}
	cdiff, err := a.diffConfig(ctx, topicName, new.Config, old.Config)
	if err != nil {
		return errors.WithStack(err)
	}
	a.logger.Sugar().Infow("Start Operation", "Name", "AlterTopicConfigs", "Topic", topicName)
	if len(cdiff) > 0 {
		responses, err := a.kafkaClient.AlterTopicConfigs(ctx, cdiff, topicName)
		if err != nil {
			return errors.WithStack(err)
		}
		response, err := responses.On(topicName, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		if response.Err != nil {
			return errors.WithStack(response.Err)
		}
	}

	udiff := a.diffUsers(old.Name, old, new)

	// Perform deletes first so that the updates performed via a delete operation
	// followed by an add are handled correctly.
	// e.g. When user ARN is modified we delete the old user and create a new one.
	for _, u := range udiff.DeletedUsers {
		err := a.userManager.DeleteUser(ctx, u, kmsKeyID, topicName, shortStackID, old.ClusterArn)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Wait until resources for deleted users are completely wiped.
	// Otherwise, next step may fail.
	// TODO: Make this wait deterministic by interrogating Secrets Manager.
	if len(udiff.DeletedUsers) > 0 {
		a.fixedDelay()
	}

	for _, u := range udiff.AddedUsers {
		err := a.userManager.CreateUser(ctx, shortStackID, topicName, kmsKeyID, old.ClusterArn, u)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	for u, aacls := range udiff.AddedPermissions {
		err := a.userManager.CreateACLs(ctx, topicName, u, shortStackID, aacls)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	for u, dacls := range udiff.DeletedPermissions {
		err := a.userManager.DeleteACLs(ctx, topicName, u, shortStackID, dacls)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (a *cmdUpdate) diffConfig(ctx context.Context, topic string, new, old map[string]*string) ([]kadm.AlterConfig, error) {
	c, err := a.kafkaClient.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	current := make(map[string]*string)
	for _, e := range c[0].Configs {
		current[e.Key] = e.Value
	}

	updates := make([]kadm.AlterConfig, 0)
	for k, nv := range new {
		if cv, ok := current[k]; ok {
			if *nv != *cv {
				updates = append(updates, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: nv})
			}
		} else {
			updates = append(updates, kadm.AlterConfig{Op: kadm.AppendConfig, Name: k, Value: nv})
		}
	}
	for k, ov := range old {
		if _, ok := new[k]; !ok {
			if cv, ok := current[k]; ok {
				if *ov != *cv {
					a.logger.Sugar().Infow("Ignore delete because current value does not match", "Name", k, "Value", *ov, "CurrentValue", *cv)
					continue
				}
			}
			updates = append(updates, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k, Value: ov})
		}
	}
	for _, u := range updates {
		a.logger.Sugar().Infow("Config Update Detected", "Name", u.Name, "Op", u.Op, "Value", *u.Value)
	}
	return updates, nil
}

func (a *cmdUpdate) diffUsers(topic string, old, new *types.TopicInfo) *userDiff {
	diff := newUserDiff()

	// First index the users in old and new configs so that
	// we can look it up in o(1)
	oldIndex := make(map[string]*types.User)
	newIndex := make(map[string]*types.User)

	for _, o := range old.Users {
		v := o
		oldIndex[o.Username] = &v
	}

	for _, n := range new.Users {
		v := n
		newIndex[n.Username] = &v
	}

	// Go through users in old list and detect any updates and deletes
	for idx, o := range old.Users {
		deletedPermissions := make([]types.Permission, 0)
		addedPermissions := make([]types.Permission, 0)
		if n, ok := newIndex[o.Username]; ok {
		aa:
			for _, op := range o.Permissions {
				for _, np := range n.Permissions {
					if op == np {
						continue aa
					}
				}
				deletedPermissions = append(deletedPermissions, op)
			}
			if len(deletedPermissions) > 0 {
				diff.DeletedPermissions[o.Username] = deletedPermissions
			}

		aaa:
			for _, np := range n.Permissions {
				for _, op := range o.Permissions {
					if np == op {
						continue aaa
					}
				}
				addedPermissions = append(addedPermissions, np)
			}
			if len(addedPermissions) > 0 {
				diff.AddedPermissions[o.Username] = addedPermissions
			}

			// When a secret is created for a user with an ARN its
			// policy contains permissions granted by TR as well as
			// the ones granted by MSK during secret association.
			// We don't want to accidentally wipe the policy changes made
			// by MSK when the ARN is modified.
			// Consequently, whenever user's ARN is modified, we delete
			// and recreate the user.
			if o.Arn != n.Arn {
				diff.AddedUsers = append(diff.AddedUsers, n)
				diff.DeletedUsers = append(diff.DeletedUsers, &old.Users[idx])
			}
		} else {
			diff.DeletedUsers = append(diff.DeletedUsers, &old.Users[idx])
		}
	}

	// Go through new users and detect additions
	for idx, u := range new.Users {
		if _, ok := oldIndex[u.Username]; !ok {
			diff.AddedUsers = append(diff.AddedUsers, &new.Users[idx])
		}
	}

	return diff
}

type userDiff struct {
	AddedUsers         []*types.User
	AddedPermissions   map[string][]types.Permission
	DeletedPermissions map[string][]types.Permission
	DeletedUsers       []*types.User
}

type userDiffOption func(*userDiff)

func withAddedUsers(users []*types.User) userDiffOption {
	return func(ud *userDiff) {
		ud.AddedUsers = users
	}
}

func withAddedPermissions(username string, permissions []types.Permission) userDiffOption {
	return func(ud *userDiff) {
		ud.AddedPermissions[username] = permissions
	}
}

func withDeletedPermissions(username string, permissions []types.Permission) userDiffOption {
	return func(ud *userDiff) {
		ud.DeletedPermissions[username] = permissions
	}
}

func withDeletedUsers(users []*types.User) userDiffOption {
	return func(ud *userDiff) {
		ud.DeletedUsers = users
	}
}

func newUserDiff(options ...userDiffOption) *userDiff {
	ud := &userDiff{
		AddedUsers:         make([]*types.User, 0),
		AddedPermissions:   make(map[string][]types.Permission),
		DeletedPermissions: make(map[string][]types.Permission),
		DeletedUsers:       make([]*types.User, 0),
	}
	for _, opt := range options {
		opt(ud)
	}
	return ud
}
