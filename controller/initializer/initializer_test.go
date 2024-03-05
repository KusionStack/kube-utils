/**
 * Copyright 2023 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package initializer

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func testInitFunc(manager.Manager) (enabled bool, err error) {
	return true, nil
}

func newTestInitializer() *managerInitializer {
	initializer := New()
	initializer.Add("test1", testInitFunc)
	initializer.Add("test2", testInitFunc)
	initializer.Add("test3", testInitFunc, WithDisableByDefault())

	return initializer.(*managerInitializer)
}

func Test_Initializer(t *testing.T) {
	initializer := newTestInitializer()

	// disable override
	err := initializer.Add("test3", testInitFunc)
	assert.Error(t, err)
	// allow override
	err = initializer.Add("test3", testInitFunc, WithOverride(), WithDisableByDefault())
	assert.NoError(t, err)

	names := initializer.Knowns()
	assert.EqualValues(t, []string{"test1", "test2", "test3"}, names)
	assert.True(t, initializer.Enabled("test1"))
	assert.True(t, initializer.Enabled("test2"))
	assert.False(t, initializer.Enabled("test3"))
}

func Test_Flag(t *testing.T) {
	tests := []struct {
		name        string
		checkResult func(*assert.Assertions, *managerInitializer)
	}{
		{
			name: "*",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.True(initializer.Enabled("test1"))
				assert.True(initializer.Enabled("test2"))
				assert.False(initializer.Enabled("test3"))
				assert.Equal("test1,test2,-test3", initializer.String())
			},
		},

		{
			name: "test1,test2",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.True(initializer.Enabled("test1"))
				assert.True(initializer.Enabled("test2"))
				assert.False(initializer.Enabled("test3"))
				assert.Equal("test1,test2,-test3", initializer.String())
			},
		},
		{
			name: "test3",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.False(initializer.Enabled("test1"))
				assert.False(initializer.Enabled("test2"))
				assert.True(initializer.Enabled("test3"))
				assert.Equal("-test1,-test2,test3", initializer.String())
			},
		},
		{
			name: "-test1",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.False(initializer.Enabled("test1"))
				assert.False(initializer.Enabled("test2"))
				assert.False(initializer.Enabled("test3"))
				assert.Equal("-test1,-test2,-test3", initializer.String())
			},
		},
		{
			name: "*,test3",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.True(initializer.Enabled("test1"))
				assert.True(initializer.Enabled("test2"))
				assert.True(initializer.Enabled("test3"))
				assert.Equal("test1,test2,test3", initializer.String())
			},
		},
		{
			name: "*,-test1",
			checkResult: func(assert *assert.Assertions, initializer *managerInitializer) {
				assert.False(initializer.Enabled("test1"))
				assert.True(initializer.Enabled("test2"))
				assert.False(initializer.Enabled("test3"))
				assert.Equal("-test1,test2,-test3", initializer.String())
			},
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			initializer := newTestInitializer()
			// test bind flag
			fs := pflag.NewFlagSet("test", pflag.PanicOnError)
			initializer.BindFlag(fs)
			fs.Set(defaultName, tt.name)
			err := fs.Parse(nil)
			assert.NoError(t, err)
			tt.checkResult(assert.New(t), initializer)
		})
	}
}
