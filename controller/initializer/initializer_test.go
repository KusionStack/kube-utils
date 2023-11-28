// Copyright 2023 The KusionStack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initializer

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func Test_Initialzier(t *testing.T) {
	initializer := New()
	err := initializer.Add("test1", testInitFunc)
	assert.NoError(t, err)
	err = initializer.Add("test2", testInitFunc)
	assert.NoError(t, err)
	err = initializer.Add("test3", testInitFunc, WithDisableByDefault())
	assert.NoError(t, err)

	controllers := initializer.KnownControllers()
	assert.EqualValues(t, []string{"test1", "test2", "test3"}, controllers)
	assert.True(t, initializer.Enabled("test1"))
	assert.True(t, initializer.Enabled("test2"))
	assert.False(t, initializer.Enabled("test3"))

	// duplicate
	err = initializer.Add("test1", testInitFunc)
	assert.Error(t, err)

	// test bind flag
	fs := pflag.NewFlagSet("test-*", pflag.PanicOnError)
	initializer.BindFlag(fs)
	fs.Set("controllers", "*")
	err = fs.Parse(nil)
	assert.NoError(t, err)
	assert.True(t, initializer.Enabled("test1"))
	assert.True(t, initializer.Enabled("test2"))
	assert.False(t, initializer.Enabled("test3"))

	fs = pflag.NewFlagSet("test", pflag.PanicOnError)
	initializer.BindFlag(fs)
	fs.Set("controllers", "test1,test2")
	err = fs.Parse(nil)
	assert.NoError(t, err)
	assert.True(t, initializer.Enabled("test1"))
	assert.True(t, initializer.Enabled("test2"))
	assert.False(t, initializer.Enabled("test3"))

	fs = pflag.NewFlagSet("test", pflag.PanicOnError)
	initializer.BindFlag(fs)
	fs.Set("controllers", "-test1,test3")
	err = fs.Parse(nil)
	assert.NoError(t, err)
	assert.False(t, initializer.Enabled("test1"))
	assert.False(t, initializer.Enabled("test2"))
	assert.True(t, initializer.Enabled("test3"))

	fs = pflag.NewFlagSet("test", pflag.PanicOnError)
	initializer.BindFlag(fs)
	fs.Set("controllers", "-test1")
	err = fs.Parse(nil)
	assert.NoError(t, err)
	assert.False(t, initializer.Enabled("test1"))
	assert.False(t, initializer.Enabled("test2"))
	assert.False(t, initializer.Enabled("test3"))
}

func testInitFunc(manager.Manager) (enabled bool, err error) {
	return true, nil
}
