package state_machine

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type logging interface {
	Debugf(format string, args ...interface{})
}

type State int

type NextStateMap struct {
	States map[string]State
}

type Configuration struct {
	State              State        `yaml:"state"`
	NextState          NextStateMap `yaml:"nextState,omitempty"`
	ConditionTimeoutMs Condition    `yaml:"conditionTimeoutMs,omitempty"`
	NextStateByTimeout State        `yaml:"nextStateByTimeout,omitempty"`
	OutMessage         string       `yaml:"outMessage,omitempty"`
	OutTag             int          `yaml:"outTag,omitempty"`
}

// UnmarshalYAML is used to unmarshal into map[string]string
func (b *NextStateMap) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return unmarshal(&b.States)
}

// StateList [condition] -> state
type StateList map[Condition]State
type StateMatrix map[State]StateStruct

const StateNil State = -1
const StateInit State = 0

type StateStruct struct {
	ThisState          State
	nextState          StateList
	conditionTimeoutMs time.Duration
	nextStateByTimeout State
	OutMessage         string
	OutTag             int
}

type StateMachine struct {
	curState     State
	stateMatrix  StateMatrix
	timer        *time.Timer
	stateHandler func(StateStruct)
	log          logging
}

func New(logger logging, stateHandler func(state StateStruct)) *StateMachine {
	return &StateMachine{
		curState:     StateInit,
		stateMatrix:  make(StateMatrix),
		timer:        nil,
		log:          logger,
		stateHandler: stateHandler,
	}
}

func (s *StateMachine) LoadConfiguration(configurationFile string) error {
	var config []Configuration

	f, err := os.Open(configurationFile)
	if err != nil {
		return err
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		return err
	}

	for _, state := range config {
		nextStateList := make(StateList)
		for resource, nextState := range state.NextState.States {
			condition := ConditionFromName(resource)
			nextStateList[condition] = nextState
		}

		if state.ConditionTimeoutMs == 0 {
			s.AddState(state.State, nextStateList)
		} else {
			s.AddStateWithTimeout(state.State, nextStateList, time.Duration(state.ConditionTimeoutMs), state.NextStateByTimeout)
		}

		if state.OutTag != 0 {
			s.AddOut(state.State, state.OutMessage, state.OutTag)
		}
	}
	return nil
}

func (s *StateMachine) AddState(newState State, nextState StateList) {
	s.stateMatrix[newState] = StateStruct{ThisState: newState, nextState: nextState, conditionTimeoutMs: 0, nextStateByTimeout: StateNil}
}

func (s *StateMachine) AddStateWithTimeout(newState State, nextState StateList, timeoutMs time.Duration, nextStateByTimeout State) {
	s.stateMatrix[newState] = StateStruct{ThisState: newState, nextState: nextState, conditionTimeoutMs: timeoutMs, nextStateByTimeout: nextStateByTimeout}
}

func (s *StateMachine) AddOut(state State, outMessage string, outTag int) {
	_state := s.stateMatrix[state]
	_state.OutMessage = outMessage
	_state.OutTag = outTag
	s.stateMatrix[state] = _state
}

func (s *StateMachine) moveToState(newState State, condition Condition) error {

	s.log.Debugf("%d-%s->%d", s.curState, condition.Name(), newState)

	if state, exists := s.stateMatrix[newState]; exists {
		s.curState = newState
		if state.conditionTimeoutMs != 0 && state.nextStateByTimeout != StateNil {
			s.timer = time.AfterFunc(state.conditionTimeoutMs*time.Millisecond, s.timeoutWorker)
		}

		s.stateHandler(state)

		if nextState := s.getUnconditionalNextState(); nextState != StateNil {
			return s.moveToState(nextState, ConditionIsNotDefine)
		}
	} else {
		return fmt.Errorf("next state %d was not found for state ", newState)
	}
	return nil
}

func (s *StateMachine) getUnconditionalNextState() State {
	if state, exists := s.stateMatrix[s.curState]; exists {
		if nextState, exists := state.nextState[0]; exists {
			return nextState
		}
	}
	return StateNil
}

func (s *StateMachine) NextState(condition Condition) error {
	state := s.stateMatrix[s.curState]

	if nextState, exists := state.nextState[condition]; exists {
		if s.timer != nil {
			s.timer.Stop()
		}
		return s.moveToState(nextState, condition)
	} else {
		return fmt.Errorf("next state was not found for state %d and condition %d", s.curState, condition)
	}
}

func (s *StateMachine) Start(state State) {
	s.curState = state
}

func (s *StateMachine) timeoutWorker() {
	state := s.stateMatrix[s.curState]
	_ = s.moveToState(state.nextStateByTimeout, ConditionTimeout)
}

func (s *StateMachine) GetAsGraphMl() string {
	var graphMl string
	for curState, state := range s.stateMatrix {
		outline := ""
		out := ""
		if state.OutMessage != "" {
			outline = "\n      outlineWidth 6\n"
			out = ":" + state.OutMessage
		}

		if curState == s.curState {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d%s\"\n    graphics\n      [\n      w 40.0\n      h 40.0\n      type \"ellipse\"\n      fill \"#FF0000\"\n%s    ]\n]\n",
				curState, curState, out, outline)
		} else {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d%s\"\n    graphics\n      [\n      w 30.0\n      h 30.0\n      type \"ellipse\"\n%s    ]\n]\n",
				curState, curState, out, outline)
		}
	}

	for curState, state := range s.stateMatrix {
		if state.conditionTimeoutMs != 0 && state.nextStateByTimeout != StateNil {
			graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    label \"T:%dms\"\n    graphics\n    [\n      style \"dashed\"\n      fill  \"#3366FF\"\n      targetArrow \"standard\"\n    ]\n  ]\n",
				curState, state.nextStateByTimeout, state.conditionTimeoutMs)
		}

		for condition, nextState := range state.nextState {
			if condition != 0 {
				graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    label \"%s\"\n  ]\n",
					curState, nextState, condition.Name())
			} else {
				graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    graphics\n    [\n      sourceArrow  \"white_circle\"\n      targetArrow  \"standard\"\n    ]\n  ]\n",
					curState, nextState)
			}
		}
	}

	return "graph [\n" + graphMl + "]\n"
}
