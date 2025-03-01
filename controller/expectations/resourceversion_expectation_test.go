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

package expectations

import (
	"github.com/stretchr/testify/suite"
)

type resourceVersionExpactationTestSuite struct {
	suite.Suite

	testKey     string
	expectation *ResourceVersionExpectation
}

func (s *resourceVersionExpactationTestSuite) SetupSuite() {
	s.testKey = "test"
}

func (s *resourceVersionExpactationTestSuite) SetupTest() {
	s.expectation = NewResourceVersionExpectation()
}

func (s *resourceVersionExpactationTestSuite) TearDownTest() {
	s.expectation = nil
}

func (s *resourceVersionExpactationTestSuite) TestExpectations_General() {
	_, ok, err := s.expectation.GetExpectations(s.testKey)
	s.NoError(err)
	s.False(ok)

	// set
	s.expectation.SetExpectations(s.testKey, "1")
	_, ok, err = s.expectation.GetExpectations(s.testKey)
	s.NoError(err)
	s.True(ok)

	// satisfied
	satisfied := s.expectation.SatisfiedExpectations(s.testKey, "0")
	s.False(satisfied)

	satisfied = s.expectation.SatisfiedExpectations(s.testKey, "1")
	s.True(satisfied)

	// delete expectations
	s.expectation.DeleteExpectations(s.testKey)
	_, ok, err = s.expectation.GetExpectations(s.testKey)
	s.NoError(err)
	s.False(ok)

	// expect update
	s.expectation.ExpectUpdate(s.testKey, "2")
	satisfied = s.expectation.SatisfiedExpectations(s.testKey, "2")
	s.True(satisfied)
}
