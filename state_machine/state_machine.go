package state_machine

import (
	"errors"
	"fmt"
	"time"
)

type State int

const StateNil State = -1
const StateInit State = 0

type StateStruct struct {
	thisState          State
	nextState          map[int]State
	timeoutMs          time.Duration
	nextStateByTimeout State
}

type StateMachine struct {
	curState     State
	stateMatrix  map[State]StateStruct
	timer        *time.Timer
	StateHandler func(thisState State)
}

func New() *StateMachine {
	return &StateMachine{
		curState:    StateInit,
		stateMatrix: make(map[State]StateStruct),
		timer:       nil,
	}
}

func (s *StateMachine) AddState(newState State, nextState map[int]State) {
	s.stateMatrix[newState] = StateStruct{thisState: newState, nextState: nextState, timeoutMs: 0, nextStateByTimeout: StateNil}
}

func (s *StateMachine) SetTimeout(curState State, timeoutMs time.Duration, nextStateByTimeout State) error {
	if state, exists := s.stateMatrix[curState]; exists {
		state.timeoutMs = timeoutMs
		state.nextStateByTimeout = nextStateByTimeout
		s.stateMatrix[curState] = state
	} else {
		return errors.New(fmt.Sprintf("state %d was not found", curState))
	}
	return nil
}

func (s *StateMachine) MoveToState(newState State) error {
	fmt.Printf("%d->%d\n", s.curState, newState)

	if state, exists := s.stateMatrix[newState]; exists {
		s.curState = newState
		if state.timeoutMs != 0 && state.nextStateByTimeout != StateNil {
			s.timer = time.AfterFunc(state.timeoutMs*time.Millisecond, s.timeoutWorker)
		}
		s.StateHandler(newState)
		if nextState := s.unconditionalMoveTo(); nextState != StateNil {
			return s.MoveToState(nextState)
		}
	} else {
		return errors.New(fmt.Sprintf("next state %d was not found for state ", newState))
	}
	return nil
}

func (s *StateMachine) unconditionalMoveTo() State {
	if state, exists := s.stateMatrix[s.curState]; exists {
		if nextState, exists := state.nextState[0]; exists {
			return nextState
		}
	}
	return StateNil
}

func (s *StateMachine) NextState(condition int) error {
	state := s.stateMatrix[s.curState]

	if nextState, exists := state.nextState[condition]; exists {
		if s.timer != nil {
			s.timer.Stop()
		}
		return s.MoveToState(nextState)
	} else {
		return errors.New(fmt.Sprintf("next state was not found for state %d and condition %d", s.curState, condition))
	}
}

func (s *StateMachine) Start(state State) {
	s.curState = state
}

func (s *StateMachine) timeoutWorker() {
	state := s.stateMatrix[s.curState]
	_ = s.MoveToState(state.nextStateByTimeout)
}

func (s *StateMachine) GetAsGraphMl() string {
	var graphMl string
	for curState, _ := range s.stateMatrix {
		if curState == s.curState {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d\"\n    graphics\n      [\n      w 40.0\n      h 40.0\n      type \"ellipse\"\n      fill \"#FF0000\"\n    ]\n]\n",
				curState, curState)
		} else {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d\"\n    graphics\n      [\n      w 30.0\n      h 30.0\n      type \"ellipse\"\n    ]\n]\n",
				curState, curState)
		}
	}

	for curState, state := range s.stateMatrix {
		if state.timeoutMs != 0 && state.nextStateByTimeout != StateNil {
			graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    label \"T:%dms\"\n    graphics\n    [\n      style \"dashed\"\n      fill  \"#3366FF\"\n      targetArrow \"standard\"\n    ]\n  ]\n",
				curState, state.nextStateByTimeout, state.timeoutMs)
		}

		for condition, nextState := range state.nextState {
			if condition != 0 {
				graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    label \"%d\"\n  ]\n",
					curState, nextState, condition)
			} else {
				graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    graphics\n    [\n      sourceArrow  \"white_circle\"\n      targetArrow  \"standard\"\n    ]\n  ]\n",
					curState, nextState)
			}
		}
	}

	return "graph [\n" + graphMl + "]\n"
}
