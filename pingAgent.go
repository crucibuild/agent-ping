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

package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/crucibuild/agent-ping/schema"
	"github.com/crucibuild/sdk-agent-go/agentiface"
	"github.com/crucibuild/sdk-agent-go/agentimpl"
)

// Resources represents an handler on the various data files
// Used by the agent(avro files, manifest, etc...).
var Resources http.FileSystem

// PingAgent is an implementation over the Agent implementation
// available in sdk-agent-go.
type PingAgent struct {
	*agentimpl.Agent
}

func mustOpenResources(path string) []byte {
	file, err := Resources.Open(path)

	if err != nil {
		panic(err)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		panic(err)
	}

	return content
}

// NewPingAgent creates a new instance of PingAgent.
func NewPingAgent() (agentiface.Agent, error) {
	var agentSpec map[string]interface{}

	manifest := mustOpenResources("/resources/manifest.json")

	err := json.Unmarshal(manifest, &agentSpec)

	if err != nil {
		return nil, err
	}

	impl, err := agentimpl.NewAgent(agentimpl.NewManifest(agentSpec))

	if err != nil {
		return nil, err
	}

	agent := &PingAgent{
		impl,
	}

	if err := agent.init(); err != nil {
		return nil, err
	}

	return agent, nil
}

func (a *PingAgent) register(rawAvroSchema string) error {
	s, err := agentimpl.LoadAvroSchema(rawAvroSchema, a)
	if err != nil {
		return err
	}

	a.SchemaRegister(s)

	return nil
}

func (a *PingAgent) init() error {
	a.SetDefaultConfigOption("delay", 1000)

	// registers additional CLI options
	for _, c := range a.Cli.RootCommand().Commands() {
		if c.Use == "start" {
			c.Flags().Int32("delay", 1000, "The delay for pinging")
			break
		}
	}

	// register schemas:
	var content []byte
	content = mustOpenResources("/schema/header.avro")
	if err := a.register(string(content[:])); err != nil {
		return err
	}

	content = mustOpenResources("/schema/test-command.avro")
	if err := a.register(string(content[:])); err != nil {
		return err
	}

	content = mustOpenResources("/schema/tested-event.avro")
	if err := a.register(string(content[:])); err != nil {
		return err
	}

	// register types
	if _, err := a.TypeRegister(agentimpl.NewTypeFromType("crucibuild/agent-example-go#tested-event", example.TestedEventType)); err != nil {
		return err
	}
	if _, err := a.TypeRegister(agentimpl.NewTypeFromType("crucibuild/agent-example-go#test-command", example.TestCommandType)); err != nil {
		return err
	}

	// register state callback
	a.RegisterStateCallback(a.onStateChange)

	return nil
}

func (a *PingAgent) onStateChange(state agentiface.State) error {
	switch state {
	case agentiface.StateConnected:
		// register callbacks
		_, err := a.RegisterEventCallback(map[string]interface{}{
			"type": "crucibuild/agent-example-go#tested-event",
		}, a.onTestedEvent)

		if err != nil {
			return err
		}

		// register main ping function
		a.Go(func(quit <-chan struct{}) error {
			delay, err := strconv.Atoi(a.GetConfigString("delay"))

			if err != nil {
				return err
			}

			for {
				select {
				case <-quit:
					return nil
				case <-time.After(time.Duration(delay) * time.Millisecond):
					// send command to pong agent
					cmd := &example.TestCommand{Foo: &example.Header{Z: "ok"}, Value: "ping", X: rand.Int31n(1000)}

					err := a.SendCommand("agent-pong", cmd)

					if err != nil {
						a.Error(err.Error())
					} else {
						a.Info("ping")
					}
				}
			}
		})
	case agentiface.StateDisconnected:
		println("Disconnected!!!")
	}
	return nil

}

func (a *PingAgent) onTestedEvent(ctx agentiface.EventCtx) error {
	a.Info("Receive tested-event: " + ctx.Message().(*example.TestedEvent).Value)

	return nil
}
