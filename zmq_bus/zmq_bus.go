package zmq_bus

import (
	"context"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

const RtdbInterrogationCommand = "{\"dest\": \"rtdb\", \"cmd\": \"gi\"}"

type ZmqBus struct {
	sync.Mutex
	ctx        *zmq.Context
	socketSub  []*zmq.Socket
	socketPub  []*zmq.Socket
	wg         sync.WaitGroup
	handler    []func([]string)
	subIdx     int
	pubIdx     int
	handlerIdx int
}

func New(numSubHandlers int, numPublishers int) (*ZmqBus, error) {
	ctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	return &ZmqBus{
		ctx:        ctx,
		socketSub:  make([]*zmq.Socket, numSubHandlers),
		socketPub:  make([]*zmq.Socket, numPublishers),
		subIdx:     0,
		pubIdx:     0,
		handler:    make([]func([]string), numSubHandlers),
		handlerIdx: 0}, err
}

func (s *ZmqBus) AddSubscriber(endpoint string) (int, error) {
	socket, err := s.ctx.NewSocket(zmq.SUB)
	if err != nil {
		return -1, err
	}

	err = socket.SetSubscribe("")
	if err != nil {
		return -1, err
	}

	err = socket.Connect(endpoint)
	if err != nil {
		return -1, err
	}

	s.socketSub[s.subIdx] = socket
	s.subIdx += 1

	return s.subIdx - 1, nil
}

func (s *ZmqBus) AddBindSubscriber(endpoint string) (int, error) {
	socket, err := s.ctx.NewSocket(zmq.SUB)
	if err != nil {
		return -1, err
	}

	err = socket.SetSubscribe("")
	if err != nil {
		return -1, err
	}

	err = socket.Bind(endpoint)
	if err != nil {
		return -1, err
	}

	s.socketSub[s.subIdx] = socket
	s.subIdx += 1

	return s.subIdx - 1, nil
}

func (s *ZmqBus) AddPublisher(endpoint string) (int, error) {
	socket, err := s.ctx.NewSocket(zmq.PUB)
	if err != nil {
		return -1, err
	}

	err = socket.Connect(endpoint)
	if err != nil {
		return -1, err
	}

	s.socketPub[s.pubIdx] = socket
	s.pubIdx += 1
	return s.pubIdx - 1, nil
}

func (s *ZmqBus) receiveHandler() error {
	s.Lock()
	handlerIdx := s.handlerIdx
	s.handlerIdx += 1
	s.Unlock()

	for {
		msg, err := s.socketSub[handlerIdx].RecvMessage(zmq.DONTWAIT)
		if err == nil {
			s.handler[handlerIdx](msg)
		} else if zmq.AsErrno(err) == 35 || zmq.AsErrno(err) == 11 {
			time.Sleep(10 * time.Millisecond)
		} else {
			return errors.New(fmt.Sprintf("zmq: %v (%d)", err, zmq.AsErrno(err)))
		}
	}
}

func (s *ZmqBus) SetReceiveHandler(handlerIdx int, handler func([]string)) {
	if handlerIdx < len(s.handler) {
		s.handler[handlerIdx] = handler
	}
}

func (s *ZmqBus) ReceiveLoopWithHandler(handlerIdx int) error {
	for {
		msg, err := s.socketSub[handlerIdx].RecvMessage(zmq.DONTWAIT)
		if err == nil {
			s.handler[handlerIdx](msg)
		} else if zmq.AsErrno(err) == 35 || zmq.AsErrno(err) == 11 {
			time.Sleep(10 * time.Millisecond)
		} else {
			return errors.New(fmt.Sprintf("zmq: %v (%d)", err, zmq.AsErrno(err)))
		}
	}
}

func (s *ZmqBus) SendInterrogationCommandToRtdb(endpoint string) (int, error) {

	client, err := s.ctx.NewSocket(zmq.PUB)
	if err != nil {
		return 0, err
	}

	defer func(client *zmq.Socket) {
		_ = client.Close()
	}(client)

	err = client.Connect(endpoint)
	if err != nil {
		return 0, err
	}

	// A delay is required before data is sent to zmq
	time.Sleep(4 * time.Second)

	return client.Send(RtdbInterrogationCommand, 0)
}

func (s *ZmqBus) Send(publisherIdx int, data []byte) (int, error) {
	s.Lock()
	count, err := s.socketPub[publisherIdx].Send(string(data), 0)
	s.Unlock()

	return count, err
}

func (s *ZmqBus) WaitingLoop() error {

	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < len(s.handler); i++ {
		g.Go(func() error {
			return s.receiveHandler()
		})
	}
	return g.Wait()
}
