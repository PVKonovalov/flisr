package state_machine

import (
	"errors"
	"fmt"
	"time"
)

type State int

const StateNil State = -1
const StateInit State = 0
const StateAlarmReceived State = 1
const State2 State = 2
const State3 State = 3
const State4 State = 4
const State5 State = 5
const State6 State = 6
const State7 State = 7

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
	fmt.Printf("->%d\n", newState)

	if state, exists := s.stateMatrix[newState]; exists {
		s.curState = newState
		if state.timeoutMs != 0 && state.nextStateByTimeout != StateNil {
			s.timer = time.NewTimer(state.timeoutMs * time.Millisecond)
			go s.timeoutWorker()
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
	<-s.timer.C
	state := s.stateMatrix[s.curState]
	_ = s.MoveToState(state.nextStateByTimeout)
}
