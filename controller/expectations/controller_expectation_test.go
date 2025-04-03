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

type controllerExpactationTestSuite struct {
	suite.Suite

	testKey     string
	expectation *ControllerExpectations
}

func (s *controllerExpactationTestSuite) SetupSuite() {
	s.testKey = "test"
}

func (s *controllerExpactationTestSuite) SetupTest() {
	s.expectation = NewControllerExpectations()
}

func (s *controllerExpactationTestSuite) TearDownTest() {
	s.expectation = nil
}

func (s *controllerExpactationTestSuite) TestExpectations_General() {
	_, ok, err := s.expectation.GetExpectations(s.testKey)
	s.Require().NoError(err)
	s.False(ok)

	// set
	s.expectation.SetExpectations(s.testKey, 1, 1)
	exp, ok, err := s.expectation.GetExpectations(s.testKey)
	s.Require().NoError(err)
	s.True(ok)
	s.EqualValues(1, exp.add)
	s.EqualValues(1, exp.del)

	// raise expectation
	s.expectation.RaiseExpectations(s.testKey, 1, 1)
	exp, ok, err = s.expectation.GetExpectations(s.testKey)
	s.Require().NoError(err)
	s.True(ok)
	s.EqualValues(2, exp.add)
	s.EqualValues(2, exp.del)

	// lower expectations
	s.expectation.LowerExpectations(s.testKey, 2, 2)
	exp, ok, err = s.expectation.GetExpectations(s.testKey)
	s.Require().NoError(err)
	s.True(ok)
	s.EqualValues(0, exp.add)
	s.EqualValues(0, exp.del)

	// satisfied
	satisfied := s.expectation.SatisfiedExpectations(s.testKey)
	s.True(satisfied)

	// delete expectations
	s.expectation.DeleteExpectations(s.testKey)
	_, ok, err = s.expectation.GetExpectations(s.testKey)
	s.Require().NoError(err)
	s.False(ok)
}

func (s *controllerExpactationTestSuite) TestCreations() {
	s.expectation.ExpectCreations(s.testKey, 2)
	exp, _, _ := s.expectation.GetExpectations(s.testKey)
	s.False(exp.Fulfilled())

	s.expectation.CreationObserved(s.testKey)
	exp, _, _ = s.expectation.GetExpectations(s.testKey)
	s.False(exp.Fulfilled())
	s.EqualValues(1, exp.add)

	s.expectation.CreationObserved(s.testKey)
	exp, _, _ = s.expectation.GetExpectations(s.testKey)
	s.True(exp.Fulfilled())
	s.EqualValues(0, exp.add)
}

func (s *controllerExpactationTestSuite) TestDeletions() {
	s.expectation.ExpectDeletions(s.testKey, 2)
	exp, _, _ := s.expectation.GetExpectations(s.testKey)
	s.False(exp.Fulfilled())

	s.expectation.DeletionObserved(s.testKey)
	exp, _, _ = s.expectation.GetExpectations(s.testKey)
	s.False(exp.Fulfilled())
	s.EqualValues(1, exp.del)

	s.expectation.DeletionObserved(s.testKey)
	exp, _, _ = s.expectation.GetExpectations(s.testKey)
	s.True(exp.Fulfilled())
	s.EqualValues(0, exp.del)
}
