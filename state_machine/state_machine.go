package state_machine

import (
	"fmt"
	"time"
)

type logging interface {
	Debugf(format string, args ...interface{})
}

type State int

// StateList [condition] -> state
type StateList map[int]State
type StateMatrix map[State]StateStruct

const StateNil State = -1
const StateInit State = 0

type StateStruct struct {
	thisState          State
	nextState          StateList
	conditionTimeoutMs time.Duration
	nextStateByTimeout State
}

type StateMachine struct {
	curState     State
	stateMatrix  StateMatrix
	timer        *time.Timer
	stateHandler func(State)
	log          logging
}

func New(logger logging, stateHandler func(state State)) *StateMachine {
	return &StateMachine{
		curState:     StateInit,
		stateMatrix:  make(StateMatrix),
		timer:        nil,
		log:          logger,
		stateHandler: stateHandler,
	}
}

func (s *StateMachine) AddState(newState State, nextState StateList) {
	s.stateMatrix[newState] = StateStruct{thisState: newState, nextState: nextState, conditionTimeoutMs: 0, nextStateByTimeout: StateNil}
}

func (s *StateMachine) AddStateWithTimeout(newState State, nextState StateList, timeoutMs time.Duration, nextStateByTimeout State) {
	s.stateMatrix[newState] = StateStruct{thisState: newState, nextState: nextState, conditionTimeoutMs: timeoutMs, nextStateByTimeout: nextStateByTimeout}
}

func (s *StateMachine) MoveToState(newState State) error {

	s.log.Debugf("%d->%d", s.curState, newState)

	if state, exists := s.stateMatrix[newState]; exists {
		s.curState = newState
		if state.conditionTimeoutMs != 0 && state.nextStateByTimeout != StateNil {
			s.timer = time.AfterFunc(state.conditionTimeoutMs*time.Millisecond, s.timeoutWorker)
		}

		s.stateHandler(newState)

		if nextState := s.unconditionalMoveTo(); nextState != StateNil {
			return s.MoveToState(nextState)
		}
	} else {
		return fmt.Errorf("next state %d was not found for state ", newState)
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
		return fmt.Errorf("next state was not found for state %d and condition %d", s.curState, condition)
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
	for curState := range s.stateMatrix {
		if curState == s.curState {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d\"\n    graphics\n      [\n      w 40.0\n      h 40.0\n      type \"ellipse\"\n      fill \"#FF0000\"\n    ]\n]\n",
				curState, curState)
		} else {
			graphMl += fmt.Sprintf("  node [\n    id %d\n    label \"%d\"\n    graphics\n      [\n      w 30.0\n      h 30.0\n      type \"ellipse\"\n    ]\n]\n",
				curState, curState)
		}
	}

	for curState, state := range s.stateMatrix {
		if state.conditionTimeoutMs != 0 && state.nextStateByTimeout != StateNil {
			graphMl += fmt.Sprintf("  edge [\n    source %d\n    target %d\n    label \"T:%dms\"\n    graphics\n    [\n      style \"dashed\"\n      fill  \"#3366FF\"\n      targetArrow \"standard\"\n    ]\n  ]\n",
				curState, state.nextStateByTimeout, state.conditionTimeoutMs)
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
