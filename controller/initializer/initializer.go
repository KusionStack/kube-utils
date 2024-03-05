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

var defaultName = "controllers"

// InitFunc is used to setup manager, particularly to launch a controller.
// It may run additional "should I activate checks".
// Any error returned will cause the main process to `Fatal`
// The bool indicates whether the controller was enabled.
type InitFunc func(manager.Manager) (enabled bool, err error)

// InitOption configures how we set up initializer
type InitOption interface {
	apply(*options)
}

type options struct {
	disableByDefault bool
	override         bool
}
type optionFunc func(*options)

func (o optionFunc) apply(opt *options) {
	o(opt)
}

// WithDisableByDefault disable controller by default.
func WithDisableByDefault() InitOption {
	return optionFunc(func(o *options) {
		o.disableByDefault = true
	})
}

// WithOverride allows overriding initializer.
func WithOverride() InitOption {
	return optionFunc(func(o *options) {
		o.override = true
	})
}

// Interface knows how to set up initializers with manager
type Interface interface {
	// Add add new setup function to initializer.
	Add(name string, setup InitFunc, options ...InitOption) error

	// Knowns returns a slice of strings describing all known initializers.
	Knowns() []string

	// Enabled returns true if the controller is enabled.
	Enabled(name string) bool

	// SetupWithManager add all enabled initializers to manager
	SetupWithManager(mgr manager.Manager) error

	// BindFlag adds a flag for setting global feature gates to the specified FlagSet.
	BindFlag(fs *pflag.FlagSet)
}

// New returns a new instance of initializer interface
func New() Interface {
	return NewNamed(defaultName)
}

// NewNamed returns a new instance of initializer interface with the specified name.
// The name will be used as the flag name in BindFlags.
func NewNamed(name string) Interface {
	return &managerInitializer{
		name:             name,
		initializers:     make(map[string]InitFunc),
		all:              sets.NewString(),
		enabled:          sets.NewString(),
		disableByDefault: sets.NewString(),
	}
}

var _ Interface = &managerInitializer{}

type managerInitializer struct {
	lock sync.RWMutex

	name             string
	initializers     map[string]InitFunc
	all              sets.String
	enabled          sets.String
	disableByDefault sets.String
}

func (m *managerInitializer) deleteUnlocked(name string) {
	if m.all.Has(name) {
		m.all.Delete(name)
		m.enabled.Delete(name)
		m.disableByDefault.Delete()
		delete(m.initializers, name)
	}
}

func (m *managerInitializer) Add(name string, setup InitFunc, opts ...InitOption) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	opt := &options{}
	for _, o := range opts {
		o.apply(opt)
	}

	// cleanup if exists
	if opt.override {
		m.deleteUnlocked(name)
	}

	if m.all.Has(name) {
		return fmt.Errorf("initializer %q has already registered", name)
	}

	m.all.Insert(name)

	if opt.disableByDefault {
		m.disableByDefault.Insert(name)
	} else {
		m.enabled.Insert(name)
	}

	m.initializers[name] = setup
	return nil
}

func (m *managerInitializer) BindFlag(fs *pflag.FlagSet) {
	all := m.all.List()
	disabled := m.disableByDefault.List()

	usage := fmt.Sprintf(""+
		"A list of %s to enable.\n"+
		"'*' enables all on-by-default %s.\n"+
		"'foo' enables the %s named 'foo'.\n"+
		"'-foo' disables the %s named 'foo'.\n"+
		"All: '%s'\n"+
		"Disabled-by-default: '%s'\n",
		m.name, m.name, m.name, m.name, strings.Join(all, ", "), strings.Join(disabled, ", "))

	flag := fs.VarPF(m, m.name, "", usage)
	flag.DefValue = "*"
}

// Knowns implements Controllerinitializer.
func (m *managerInitializer) Knowns() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.all.List()
}

func (m *managerInitializer) SetupWithManager(mgr manager.Manager) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, name := range m.enabled.List() {
		_, err := m.initializers[name](mgr)
		if err != nil {
			return fmt.Errorf("failed to initialize %q: %v", name, err)
		}
	}
	return nil
}

func (m *managerInitializer) Enabled(name string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.enabled.Has(name)
}

func (m *managerInitializer) enabledUnlocked(name string, flagValue []string) bool {
	hasStar := false
	for _, ctrl := range flagValue {
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
func (m *managerInitializer) Set(value string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	initializers := strings.Split(strings.TrimSpace(value), ",")
	all := m.all.List()
	for _, name := range all {
		if m.enabledUnlocked(name, initializers) {
			m.enabled.Insert(name)
		} else {
			m.enabled.Delete(name)
		}
	}
	return nil
}

// Type implements pflag.Value interface.
func (m *managerInitializer) Type() string {
	return "stringSlice"
}

// String implements pflag.Value interface.
func (m *managerInitializer) String() string {
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
