/**
 * Copyright 2023 The KusionStack Authors
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
	"fmt"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// InitFunc is used to launch a particular controller.  It may run additional "should I activate checks".
// Any error returned will cause the controller process to `Fatal`
// The bool indicates whether the controller was enabled.
type InitFunc func(manager.Manager) (enabled bool, err error)

// InitOption configures how we set up initializer
type InitOption interface {
	apply(*options)
}

type options struct {
	disableByDefault bool
}
type optionFunc func(*options)

func (o optionFunc) apply(opt *options) {
	o(opt)
}

// WithDisableByDefault disable controller by default
func WithDisableByDefault() InitOption {
	return optionFunc(func(o *options) {
		o.disableByDefault = true
	})
}

// Interface knows how to set up controllers with manager
type Interface interface {
	// Add add new controller setup function to initializer.
	Add(controllerName string, setup InitFunc, options ...InitOption) error

	// KnownControllers returns a slice of strings describing the ControllerInitialzier's known controllers.
	KnownControllers() []string

	// Enabled returns true if the controller is enabled.
	Enabled(name string) bool

	// SetupWithManager add all enabled controllers to manager
	SetupWithManager(mgr manager.Manager) error

	// BindFlag adds a flag for setting global feature gates to the specified FlagSet.
	BindFlag(fs *pflag.FlagSet)
}

// New returns a new instance of initializer interface
func New() Interface {
	return &controllerInitializer{
		initializers:     make(map[string]InitFunc),
		all:              sets.NewString(),
		enabled:          sets.NewString(),
		disableByDefault: sets.NewString(),
	}
}

var _ Interface = &controllerInitializer{}

type controllerInitializer struct {
	lock sync.RWMutex

	initializers     map[string]InitFunc
	all              sets.String
	enabled          sets.String
	disableByDefault sets.String
}

func (m *controllerInitializer) Add(controllerName string, setup InitFunc, opts ...InitOption) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.all.Has(controllerName) {
		return fmt.Errorf("controller %q has already been registered", controllerName)
	}

	opt := &options{}
	for _, o := range opts {
		o.apply(opt)
	}

	m.all.Insert(controllerName)

	if opt.disableByDefault {
		m.disableByDefault.Insert(controllerName)
	} else {
		m.enabled.Insert(controllerName)
	}

	m.initializers[controllerName] = setup
	return nil
}

func (m *controllerInitializer) BindFlag(fs *pflag.FlagSet) {
	all := m.all.List()
	disabled := m.disableByDefault.List()
	fs.Var(m, "controllers", fmt.Sprintf(""+
		"A list of controllers to enable. '*' enables all on-by-default controllers, 'foo' enables the controller "+
		"named 'foo', '-foo' disables the controller named 'foo'.\nAll controllers: %s\nDisabled-by-default controllers: %s",
		strings.Join(all, ", "), strings.Join(disabled, ", ")))
}

// KnownControllers implements ControllerInitialzier.
func (m *controllerInitializer) KnownControllers() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.all.List()
}

func (m *controllerInitializer) SetupWithManager(mgr manager.Manager) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, name := range m.enabled.List() {
		_, err := m.initializers[name](mgr)
		if err != nil {
			return fmt.Errorf("failed to initialize controller %q: %v", name, err)
		}
	}
	return nil
}

func (m *controllerInitializer) Enabled(name string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.enabled.Has(name)
}

func (m *controllerInitializer) isControllerEnabled(name string, controllers []string) bool {
	hasStar := false
	for _, ctrl := range controllers {
		if ctrl == name {
			return true
		}
		if ctrl == "-"+name {
			return false
		}
		if ctrl == "*" {
			hasStar = true
		}
	}
	// if we get here, there was no explicit choice
	if !hasStar {
		// nothing on by default
		return false
	}

	return !m.disableByDefault.Has(name)
}

// Set implements pflag.Value interface.
func (m *controllerInitializer) Set(value string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	controllers := strings.Split(strings.TrimSpace(value), ",")
	all := m.all.List()
	for _, name := range all {
		if m.isControllerEnabled(name, controllers) {
			m.enabled.Insert(name)
		} else {
			m.enabled.Delete(name)
		}
	}
	return nil
}

// Type implements pflag.Value interface.
func (m *controllerInitializer) Type() string {
	return "stringSlice"
}

// String implements pflag.Value interface.
func (m *controllerInitializer) String() string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	pairs := []string{}
	for _, name := range m.all.List() {
		if m.enabled.Has(name) {
			pairs = append(pairs, name)
		} else {
			pairs = append(pairs, "-"+name)
		}
	}
	return strings.Join(pairs, ",")
}
