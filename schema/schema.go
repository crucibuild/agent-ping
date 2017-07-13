// Copyright (C) 2016 Christophe Camel, Jonathan Pigrée
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"github.com/crucibuild/sdk-agent-go/util"
	"reflect"
)

// Header of a command.
type Header struct {
	Z string
}

// TestCommand represents a "test command" as defined in the avro file.
type TestCommand struct {
	Foo   *Header
	Value string
	X     int32
}

// TestCommandType represents the type of a TestCommand.
var TestCommandType reflect.Type

// TestedEvent represents a "tested event" as defined in the avro file.
type TestedEvent struct {
	Value string
}

// TestedEventType represents the type of a TestedEvent.
var TestedEventType reflect.Type

func init() {
	var err error

	TestCommandType, err = util.GetStructType(&TestCommand{})
	if err != nil {
		panic(err)
	}

	TestedEventType, err = util.GetStructType(&TestedEvent{})
	if err != nil {
		panic(err)
	}
}
