// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func customCreateSession(cluster *gocql.ClusterConfig) (commongocql.GocqlSession, error) {
	return nil, fmt.Errorf("not implemented, this is a test")
}

func TestCreateSessionCatalog(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	cluster := gocql.NewCluster("this-is-a-test.this-host-is-not-defined.this-domain-does-not-exist")

	createSession, err := getCreateSessionFunc("")
	assert.NoError(err)
	require.NotNil(createSession,
		"default create session func is declared")
	session, err := createSession(cluster)
	assert.Nil(session)
	require.NotNil(err)
	assert.Contains(err.Error(), "gocql:",
		"default create session func is using upstream implementation")

	createSession, err = getCreateSessionFunc("custom")
	require.NotNil(err)
	assert.Contains(err.Error(), "custom",
		"custom create session func not defined yet")
	assert.Nil(createSession)

	RegisterCreateSessionFunc("custom", customCreateSession)

	createSession, err = getCreateSessionFunc("custom")
	assert.NoError(err)
	require.NotNil(createSession,
		"custom create session is defined")
	session, err = createSession(cluster)
	assert.Nil(session)
	require.NotNil(err)
	assert.Equal("not implemented, this is a test", err.Error(),
		"custom create session func is using custom implementation")

	createSession, err = getCreateSessionFunc("")
	assert.NoError(err)
	require.NotNil(commongocql.CreateSession, createSession,
		"default create session func is still declaared")
	session, err = createSession(cluster)
	assert.Nil(session)
	require.NotNil(err)
	assert.Contains(err.Error(), "gocql:",
		"default create session func is still using upstream implementation")

	RegisterCreateSessionFunc("", customCreateSession)

	createSession, err = getCreateSessionFunc("")
	assert.NoError(err)
	require.NotNil(createSession,
		"custom create session is still defined")
	session, err = createSession(cluster)
	assert.Nil(session)
	require.NotNil(err)
	assert.Equal("not implemented, this is a test", err.Error(),
		"custom create session func is still using custom implementation")
}
