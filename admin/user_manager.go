// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	tt "tr/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smt "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

const SecretPolicyTemplate string = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "%s"
			},
			"Action": "secretsmanager:GetSecretValue",
			"Resource": "*"
		}
	]
}`
const SecretTemplate string = `{"username":"%s", "password":"%s"}`

type UserManagerService interface {
	CreateUser(ctx context.Context, shortStackID, topic, kmsKeyID, clusterArn string, u *tt.User) error
	DeleteUser(ctx context.Context, u *tt.User, kmsKeyID, topic, shortStackID, clusterArn string) error
	CreateACLs(ctx context.Context, topic, username, shortStackID string, permissions []tt.Permission) error
	DeleteACLs(ctx context.Context, topic, username, shortStackID string, permissions []tt.Permission) error
}

type userManager struct {
	secretsManagerClient SecretsManagerClient
	kmsClient            KmsClient
	mskClient            MskClient
	kafkaClient          KafkaClient
	logger               *zap.Logger
	fixedDelay           func()
}

func newUserManager(secretsManagerClient SecretsManagerClient, kmsClient KmsClient, mskClient MskClient, kafkaClient KafkaClient, logger *zap.Logger, fixedDelay func()) *userManager {
	return &userManager{
		secretsManagerClient: secretsManagerClient,
		kmsClient:            kmsClient,
		mskClient:            mskClient,
		kafkaClient:          kafkaClient,
		logger:               logger,
		fixedDelay:           fixedDelay,
	}
}

func (um *userManager) CreateUser(ctx context.Context, shortStackID, topic, kmsKeyID, clusterArn string, u *tt.User) error {
	username := canonicalUsername(u.Username, shortStackID)
	password, err := um.generatePassword()
	if err != nil {
		return errors.WithStack(err)
	}
	um.logger.Sugar().Infow("Start Operation", "Name", "CreateSecret", "Username", u.Username, "KmsKeyId", kmsKeyID)
	var secretArn string
	csr, err := um.secretsManagerClient.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(username),
		KmsKeyId:     &kmsKeyID,
		SecretString: aws.String(fmt.Sprintf(SecretTemplate, username, password)),
	})
	if err != nil {
		// If secret already exists, describe to find out its ARN
		var ral *smt.ResourceExistsException
		if !errors.As(err, &ral) {
			return errors.WithStack(err)
		}
		um.logger.Sugar().Infow("Retry Handled", "Operation", "CreateSecret", "Username", u.Username)
		ds, err := um.secretsManagerClient.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
			SecretId: &username,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		secretArn = *ds.ARN
	} else {
		secretArn = *csr.ARN
	}
	if u.Arn != "" {
		err = um.grantAccessToSecretForArn(ctx, username, kmsKeyID, secretArn, u.Arn)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Wait to ensure that Secret is created and available
	// for association with MSK.
	um.fixedDelay()

	um.logger.Sugar().Infow("Start Operation", "Name", "BatchAssociateScramSecret", "Username", username, "SecretArn", secretArn)
	bass, err := um.mskClient.BatchAssociateScramSecret(ctx, &kafka.BatchAssociateScramSecretInput{
		ClusterArn:    &clusterArn,
		SecretArnList: []string{secretArn},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if len(bass.UnprocessedScramSecrets) == 1 {
		uss := bass.UnprocessedScramSecrets[0]
		if *uss.ErrorMessage != "The provided secret is already associated with this cluster. To update the association, first disassociate the secret." {
			return errors.WithStack(fmt.Errorf("failed to associate secret: %s %s", *uss.ErrorCode, *uss.ErrorMessage))
		}
		um.logger.Sugar().Infow("Retry Handled", "Operation", "BatchAssociateScramSecret", "Username", username)
	}
	err = um.createACLs(ctx, topic, username, u.Permissions)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Performs the clean up operations for resources created in createUser in reverse order.
func (um *userManager) DeleteUser(ctx context.Context, u *tt.User, kmsKeyID, topic, shortStackID, clusterArn string) error {
	username := canonicalUsername(u.Username, shortStackID)
	err := um.deleteACLs(ctx, topic, username, u.Permissions)
	if err != nil {
		return errors.WithStack(err)
	}

	um.logger.Sugar().Infow("Start Operation", "Name", "DescribeSecret", "Username", username)
	ds, err := um.secretsManagerClient.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: &username,
	})
	if err != nil {
		var e *smt.ResourceNotFoundException
		if errors.As(err, &e) {
			// Since secret is deleted as the last action in this flow,
			// we can assume that there's no more clean-up to do for this user.
			um.logger.Sugar().Infow("Retry Handled", "Operation", "DescribeSecret")
			return nil
		} else {
			return errors.WithStack(err)
		}
	}

	um.logger.Sugar().Infow("Start Operation", "Name", "BatchDisassociateScramSecret")
	bdss, err := um.mskClient.BatchDisassociateScramSecret(ctx, &kafka.BatchDisassociateScramSecretInput{
		ClusterArn:    &clusterArn,
		SecretArnList: []string{*ds.ARN},
	})
	if err != nil {
		if isRetriable(err) {
			return errors.WithStack(err)
		}
		um.logger.Sugar().Errorw("Operation Failed", "Error", err)
	} else {
		if len(bdss.UnprocessedScramSecrets) == 1 {
			e := bdss.UnprocessedScramSecrets[0]
			if *e.ErrorMessage != "The provided secret ARN is invalid." {
				return errors.WithStack(errors.New(*bdss.UnprocessedScramSecrets[0].ErrorMessage))
			}
			um.logger.Sugar().Infow("Retry Handled", "Operation", "BatchDisassociateScramSecret")
		}
	}

	if u.Arn != "" {
		// Find GrantID by attempting to create the Grant with the same name
		um.logger.Sugar().Infow("Start Operation", "Name", "CreateGrant")
		cgo, err := um.kmsClient.CreateGrant(ctx, &kms.CreateGrantInput{
			KeyId:            &kmsKeyID,
			Name:             &username,
			Operations:       []types.GrantOperation{types.GrantOperationDecrypt},
			GranteePrincipal: &u.Arn,
		})
		if err != nil {
			if isRetriable(err) {
				return errors.WithStack(err)
			}
			um.logger.Sugar().Errorw("Operation Failed", "Error", err)
		} else {
			um.logger.Sugar().Infow("Start Operation", "Name", "RevokeGrant")
			_, err = um.kmsClient.RevokeGrant(ctx, &kms.RevokeGrantInput{
				GrantId: cgo.GrantId,
				KeyId:   &kmsKeyID,
			})
			if err != nil {
				if isRetriable(err) {
					return errors.WithStack(err)
				}
				um.logger.Sugar().Errorw("Operation Failed", "Error", err)
			}
		}
	}

	um.logger.Sugar().Infow("Start Operation", "Name", "DeleteSecret", "Username", username)
	_, err = um.secretsManagerClient.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   &username,
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	if err != nil {
		if isRetriable(err) {
			return errors.WithStack(err)
		}
		um.logger.Sugar().Errorw("Operation Failed", zap.Error(err))
	}

	return nil
}

func (um *userManager) CreateACLs(ctx context.Context, topic, username, shortStackID string, permissions []tt.Permission) error {
	username = canonicalUsername(username, shortStackID)
	return um.createACLs(ctx, topic, username, permissions)
}

func (um *userManager) createACLs(ctx context.Context, topic, username string, permissions []tt.Permission) error {
	um.logger.Sugar().Infow("Start Operation", "Name", "CreateACLs")
	acls := um.userPermissionToACL(topic, username, permissions)
	for _, acl := range acls {
		car, err := um.kafkaClient.CreateACLs(ctx, acl)
		if err != nil {
			return errors.WithStack(err)
		}
		if car[0].Err != nil {
			return errors.WithStack(car[0].Err)
		}
	}
	return nil
}

func (um *userManager) DeleteACLs(ctx context.Context, topic, username, shortStackID string, permissions []tt.Permission) error {
	return um.deleteACLs(ctx, topic, canonicalUsername(username, shortStackID), permissions)
}

func (um *userManager) userPermissionToACL(topic, username string, permissions []tt.Permission) []*kadm.ACLBuilder {
	acls := make([]*kadm.ACLBuilder, 0)
	topicACLBuilder := kadm.NewACLs().Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral)
	groupACLBuilder := kadm.NewACLs().Groups("*").ResourcePatternType(kadm.ACLPatternLiteral)
	topicOps := make([]kmsg.ACLOperation, 0)
	groupOps := make([]kadm.ACLOperation, 0)
	for _, permission := range permissions {
		if permission == tt.PermissionRead {
			topicOps = append(topicOps, kadm.OpRead)
			groupOps = append(groupOps, kadm.OpRead)
			groupOps = append(groupOps, kadm.OpDescribe)
		}
		if permission == tt.PermissionWrite {
			topicOps = append(topicOps, kadm.OpWrite)
		}
	}
	topicACLBuilder.Operations(topicOps...)
	groupACLBuilder.Operations(groupOps...)
	acls = append(acls, topicACLBuilder.Allow(fmt.Sprintf("User:%s", username)).AllowHosts("*"))
	if len(groupOps) > 0 {
		acls = append(acls, groupACLBuilder.Allow(fmt.Sprintf("User:%s", username)).AllowHosts("*"))
	}
	return acls
}

func (um *userManager) grantAccessToSecretForArn(ctx context.Context, username, kmsKeyID, secretArn, principalArn string) error {
	um.logger.Sugar().Infow("Start Operation", "Name", "PutResourcePolicy", "ARN", principalArn)
	_, err := um.secretsManagerClient.PutResourcePolicy(ctx, &secretsmanager.PutResourcePolicyInput{
		SecretId:       &secretArn,
		ResourcePolicy: aws.String(fmt.Sprintf(SecretPolicyTemplate, principalArn)),
	})
	if err != nil {
		return errors.WithStack(err)
	}

	um.logger.Sugar().Infow("Start Operation", "Name", "CreateGrant", "ARN", principalArn)
	_, err = um.kmsClient.CreateGrant(ctx, &kms.CreateGrantInput{
		Name:             &username,
		KeyId:            &kmsKeyID,
		GranteePrincipal: &principalArn,
		Operations:       []types.GrantOperation{types.GrantOperationDecrypt},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (a *userManager) deleteACLs(ctx context.Context, topic, username string, permissions []tt.Permission) error {
	a.logger.Sugar().Infow("Start Operation", "Name", "DeleteKafkaACL")
	acls := a.userPermissionToACL(topic, username, permissions)
	for _, acl := range acls {
		r, err := a.kafkaClient.DeleteACLs(ctx, acl)
		if err != nil {
			if kerr.IsRetriable(err) {
				return errors.WithStack(err)
			}
			a.logger.Sugar().Errorw("Operation Failed", "Error", err)
			return nil
		}
		if r[0].Err != nil {
			if kerr.IsRetriable(r[0].Err) {
				return errors.WithStack(r[0].Err)
			}
			a.logger.Sugar().Errorw("Operation Failed", "Error", err)
		}
	}
	return nil
}

func (a *userManager) generatePassword() (string, error) {
	buf := make([]byte, 9)
	n, err := rand.Read(buf)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if n != len(buf) {
		return "", errors.New("password generation failed")
	}
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(buf), nil
}
