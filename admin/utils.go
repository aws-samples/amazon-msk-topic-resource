// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package admin

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"fmt"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/pkg/errors"
)

func isRetriable(err error) bool {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.Response.StatusCode >= 500
	}
	return true
}

func shortStackID(stackID string) string {
	h := sha256.Sum256([]byte(stackID))
	id := base32.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(h[0:])
	return id[0:8]
}

// Canonical topic name is used to ensure that same topic name
// used in two different CF templates are not referring to the same
// topic. Canonical topic name is created by appending a short hash
// of stackID to topic name.
func canonicalTopicName(name, shortStackID string) string {
	return fmt.Sprintf("%s-%s", name, shortStackID)
}

// Canonical username is used to ensure that two usernames used in
// two different CF templates are not referring to the same user.
// Canonical user name is created by appending a short hash of stackID
// to topic name.
// If two topics in the same CF template use the same username, they
// will share the user account.
func canonicalUsername(username, shortStackID string) string {
	return fmt.Sprintf("AmazonMSK_%s_%s", username, shortStackID)
}
