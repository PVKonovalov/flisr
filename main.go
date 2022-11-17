package main

import (
	"encoding/json"
	"errors"
	"flag"
	"flisr/llog"
	"flisr/message"
	sm "flisr/state_machine"
	"flisr/topogrid"
	"flisr/types"
	"flisr/webapi"
	"flisr/zmq_bus"
	"fmt"
	"github.com/PVKonovalov/localcache"
	"gopkg.in/ini.v1"
	"strings"
	"time"
)

const ApiGetTopology = "/api/topology/graph"
const ApiGetEquipment = "/api/equipment"
const ApiTimeoutSec = 60

const ResourceStateGood = 0
const ResourceStateIsolated = 1
const ResourceStateFault = 2

const (
	StateAlarmReceived sm.State = 1
	State2             sm.State = 2
	State3             sm.State = 3
	State4             sm.State = 4
	State5             sm.State = 5
	State6             sm.State = 6
	State7             sm.State = 7
	State8             sm.State = 8
	State9             sm.State = 9
	State10            sm.State = 10
)

const OutTagReclosingOk = 1
const OutTagReclosingFailed = 2

// ConfigStruct from FLISR configuration file
type ConfigStruct struct {
	username                 string
	password                 string
	configAPIHostVirtualName string
	configAPIHostList        []string
	logLevel                 string
	zmqRtdb                  string
	zmqRtdbCommand           string
	zmqRtdbInput             string
	zmqAlarmMessage          string
	jobQueueLength           int
	rtdbPointFlisrState      uint64
	pointSource              uint32
	stateMachineConfig       string
}

type EdgeStruct struct {
	EquipmentType           string `json:"equipment_type,omitempty"`
	EquipmentName           string `json:"equipment_name,omitempty"`
	EquipmentId             int    `json:"equipment_id,omitempty"`
	EquipmentTypeId         int    `json:"equipment_type_id,omitempty"`
	EquipmentVoltageClassId int    `json:"equipment_voltage_class_id,omitempty"`
	Id                      int    `json:"id"`
	StateNormal             int    `json:"state_normal"`
	Terminal1               int    `json:"terminal1"`
	Terminal2               int    `json:"terminal2"`
}

type TopologyStruct struct {
	Edge []EdgeStruct `json:"edge"`
	Node []struct {
		EquipmentId             int    `json:"equipment_id,omitempty"`
		EquipmentName           string `json:"equipment_name,omitempty"`
		EquipmentTypeId         int    `json:"equipment_type_id,omitempty"`
		EquipmentVoltageClassId int    `json:"equipment_voltage_class_id,omitempty"`
		Id                      int    `json:"id"`
	} `json:"node"`
}

type EquipmentStruct struct {
	electricalState       uint32
	groundedFrom          map[int]bool
	energizedFrom         map[int]bool
	EquipmentType         string `json:"equipment_type,omitempty"`
	EquipmentVoltageClass string `json:"equipment_voltage_class"`
	Id                    int    `json:"id"`
	Name                  string `json:"name"`
	TypeId                int    `json:"type_id,omitempty"`
	VoltageClassId        int    `json:"voltage_class_id"`
	Resource              []struct {
		Id          int    `json:"id"`
		Point       string `json:"point"`
		PointId     uint64 `json:"point_id"`
		PointTypeId int    `json:"point_type_id"`
		Type        string `json:"type"`
		TypeId      int    `json:"type_id"`
	} `json:"resource,omitempty"`
}

type ResourceStruct struct {
	equipmentId    int
	resourceTypeId int
}

type ThisService struct {
	log                                   *llog.LevelLog
	config                                ConfigStruct
	topologyProfile                       *TopologyStruct
	topologyFlisr                         *topogrid.TopologyGridStruct
	topologyGrid                          *topogrid.TopologyGridStruct
	equipmentFromEquipmentId              map[int]EquipmentStruct
	pointFromEquipmentIdAndResourceTypeId map[int]map[int]uint64
	resourceStructFromPointId             map[uint64]ResourceStruct
	equipmentIdArrayFromResourceTypeId    map[int][]int
	zmq                                   *zmq_bus.ZmqBus
	outputEventQueue                      chan types.RtdbMessage
	inputDataQueue                        chan types.RtdbMessage
	switchDataQueue                       chan types.RtdbMessage
	outputMessageQueue                    chan message.OutputMessageStruct
	stateMachine                          *sm.StateMachine
	alarmProtectBuffer                    []types.RtdbMessage
	stateSwitchBuffer                     []types.RtdbMessage
	rtdbPublisherIdx                      int
	messagePublisherIdx                   int
}

// New FLISR service
func New() *ThisService {
	logger := llog.NewLevelLog(llog.Ldate | llog.Ltime | llog.Lmicroseconds)
	return &ThisService{
		log:                                   logger,
		equipmentFromEquipmentId:              make(map[int]EquipmentStruct),
		resourceStructFromPointId:             make(map[uint64]ResourceStruct),
		alarmProtectBuffer:                    make([]types.RtdbMessage, 0),
		pointFromEquipmentIdAndResourceTypeId: make(map[int]map[int]uint64),
		equipmentIdArrayFromResourceTypeId:    make(map[int][]int),
		stateSwitchBuffer:                     make([]types.RtdbMessage, 0),
	}
}

// ReadConfig from FLISR configuration file
func (s *ThisService) ReadConfig(configFile string) error {
	var cfg *ini.File
	var err error

	if cfg, err = ini.Load(configFile); err != nil {
		return err
	}

	s.config.username = cfg.Section("CONFIGAPI").Key("USERNAME").String()
	s.config.password = cfg.Section("CONFIGAPI").Key("PASSWORD").String()
	s.config.configAPIHostVirtualName = cfg.Section("CONFIGAPI").Key("HOST_VIRTUAL_NAME").String()
	s.config.configAPIHostList = strings.Split(cfg.Section("CONFIGAPI").Key("HOST").String(), ",")

	s.config.zmqRtdb = cfg.Section("BUSES").Key("ZMQ_RTDB_OUTPUT_POINT").String()
	s.config.zmqRtdbCommand = cfg.Section("BUSES").Key("ZMQ_RTDB_COMMAND_POINT").String()
	s.config.zmqRtdbInput = cfg.Section("BUSES").Key("ZMQ_RTDB_INPUT_POINT").String()

	s.config.zmqAlarmMessage = cfg.Section("SLD_BRIDGE").Key("ZMQ_ALARM_MESSAGE_POINT").String()

	s.config.logLevel = cfg.Section("FLISR").Key("LOG").String()
	if s.config.jobQueueLength, err = cfg.Section("FLISR").Key("QUEUE").Int(); err != nil {
		s.config.jobQueueLength = 100
	}

	s.config.stateMachineConfig = cfg.Section("FLISR").Key("STATE_MACHINE_CONFIG").String()

	var pointSource uint64
	s.config.pointSource = 0
	if pointSource, err = cfg.Section("FLISR").Key("POINT_SOURCE").Uint64(); err == nil {
		s.config.pointSource = uint32(pointSource)
	}

	if s.config.rtdbPointFlisrState, err = cfg.Section("FLISR").Key("RTDB_POINT_FLISR_STATE").Uint64(); err != nil {
		s.config.rtdbPointFlisrState = 0
	}

	return nil
}

func ParseTopologyData(data []byte) (*TopologyStruct, error) {
	var topologyStruct TopologyStruct
	err := json.Unmarshal(data, &topologyStruct)
	return &topologyStruct, err
}

func ParseEquipmentData(data []byte) (*[]EquipmentStruct, error) {
	var equipmentStructs []EquipmentStruct
	err := json.Unmarshal(data, &equipmentStructs)
	return &equipmentStructs, err
}

// LoadTopologyProfile Loading topologyProfile from configs.configAPIHostList
func (s *ThisService) LoadTopologyProfile(timeoutSec time.Duration, isLoadFromCache bool, cachePath string) error {
	var topologyData []byte

	cache := localcache.New(cachePath)

	if isLoadFromCache {
		s.log.Infof("Loading topologyProfile from local cache (%s)", cachePath)
		profileData, err := cache.Load()
		if err != nil {
			return err
		}
		s.topologyProfile, err = ParseTopologyData(profileData)
		return err
	}

	resultErr := errors.New("unknown error. Check configuration file")

	for _, urlAPIHost := range s.config.configAPIHostList {
		urlAPIHost = strings.TrimSpace(urlAPIHost)
		api := webapi.Connection{
			Timeout:         timeoutSec,
			BaseUrl:         urlAPIHost,
			HostVirtualName: s.config.configAPIHostVirtualName,
		}

		s.log.Debugf("Logon to %s as %s", api.BaseUrl, s.config.username)
		_, err, _ := api.Logon(s.config.username, s.config.password)
		if err != nil {
			s.log.Errorf("Failed to logon: %v", err)
			resultErr = err
			continue
		}

		s.log.Debugf("Getting topology profile ...")
		topologyData, err = api.GetProfile(ApiGetTopology)

		if err != nil {
			s.log.Errorf("Failed to get topology profile: %v", err)
			resultErr = err
			continue
		}

		s.topologyProfile, err = ParseTopologyData(topologyData)

		if err != nil {
			s.log.Errorf("Failed to unmarshal topology profile: %v", err)
			resultErr = err
			continue
		} else {
			resultErr = nil
			break
		}
	}

	if resultErr == nil {
		err := cache.Save(topologyData)

		if err != nil {
			s.log.Errorf("Failed to write to local cache (%s)", cachePath)
			resultErr = err
		}
	} else {
		s.log.Errorf("Failed to load topology profile from API host: %v", resultErr)
		s.log.Infof("Loading from local cache (%s)", cachePath)
		profileData, err := cache.Load()

		if err != nil {
			return err
		}

		s.topologyProfile, err = ParseTopologyData(profileData)
		resultErr = err
	}

	if cache.IsChanged {
		s.log.Infof("Configuration changed from the previous loading")
	}

	return resultErr
}

// LoadEquipmentProfile Loading equipment from config.ConfigAPIHostList
func (s *ThisService) LoadEquipmentProfile(timeoutSec time.Duration, isLoadFromCache bool, cachePath string) error {
	var equipmentData []byte
	var equipments *[]EquipmentStruct

	cache := localcache.New(cachePath)

	if isLoadFromCache {
		s.log.Infof("Loading equipment from local cache (%s)", cachePath)
		profileData, err := cache.Load()
		if err != nil {
			return err
		}
		equipments, err = ParseEquipmentData(profileData)
		if err == nil {
			for _, _equipment := range *equipments {
				s.equipmentFromEquipmentId[_equipment.Id] = _equipment
			}
		}
		return err
	}

	resultErr := errors.New("unknown error. Check configuration file")

	for _, urlAPIHost := range s.config.configAPIHostList {
		urlAPIHost = strings.TrimSpace(urlAPIHost)
		api := webapi.Connection{
			Timeout:         timeoutSec,
			BaseUrl:         urlAPIHost,
			HostVirtualName: s.config.configAPIHostVirtualName,
		}

		s.log.Debugf("Logon to %s as %s", api.BaseUrl, s.config.username)
		_, err, _ := api.Logon(s.config.username, s.config.password)
		if err != nil {
			s.log.Errorf("Failed to logon: %v", err)
			resultErr = err
			continue
		}

		s.log.Debugf("Getting equipment profile ...")
		equipmentData, err = api.GetProfile(ApiGetEquipment)

		if err != nil {
			s.log.Errorf("Failed to get equipment: %v", err)
			resultErr = err
			continue
		}

		equipments, err = ParseEquipmentData(equipmentData)

		if err != nil {
			s.log.Errorf("Failed to unmarshal equipment: %v", err)
			resultErr = err
			continue
		} else {
			for _, _equipment := range *equipments {
				s.equipmentFromEquipmentId[_equipment.Id] = _equipment
			}
			resultErr = nil
			break
		}
	}

	if resultErr == nil {
		err := cache.Save(equipmentData)

		if err != nil {
			s.log.Errorf("Failed to write to local cache (%s)", cachePath)
			resultErr = err
		}
	} else {
		s.log.Errorf("Failed to load equipment from API host: %v", resultErr)
		s.log.Infof("Loading from local cache (%s)", cachePath)
		profileData, err := cache.Load()

		if err != nil {
			return err
		}

		equipments, err = ParseEquipmentData(profileData)

		if err == nil {
			for _, _equipment := range *equipments {
				s.equipmentFromEquipmentId[_equipment.Id] = _equipment
			}
		}
		resultErr = err
	}

	if cache.IsChanged {
		s.log.Infof("Configuration changed from the previous loading")
	}

	return resultErr
}

func (s *ThisService) CreateInternalParametersFromProfiles() {
	for _, equipment := range s.equipmentFromEquipmentId {
		for _, resource := range equipment.Resource {
			if resource.TypeId == types.ResourceTypeProtect ||
				resource.TypeId == types.ResourceTypeReclosing ||
				resource.TypeId == types.ResourceTypeState ||
				resource.TypeId == types.ResourceTypeStateLineSegment {
				s.resourceStructFromPointId[resource.PointId] = ResourceStruct{
					equipmentId:    equipment.Id,
					resourceTypeId: resource.TypeId,
				}

				if _, exists := s.pointFromEquipmentIdAndResourceTypeId[equipment.Id]; !exists {
					s.pointFromEquipmentIdAndResourceTypeId[equipment.Id] = make(map[int]uint64)
				}
				s.pointFromEquipmentIdAndResourceTypeId[equipment.Id][resource.TypeId] = resource.PointId

			}
			if _, exists := s.equipmentIdArrayFromResourceTypeId[resource.TypeId]; !exists {
				s.equipmentIdArrayFromResourceTypeId[resource.TypeId] = make([]int, 0)
			}
			s.equipmentIdArrayFromResourceTypeId[resource.TypeId] = append(s.equipmentIdArrayFromResourceTypeId[resource.TypeId], equipment.Id)
		}
	}
}

func (s *ThisService) LoadTopologyGrid() error {
	s.topologyFlisr = topogrid.New(len(s.topologyProfile.Node))

	for _, node := range s.topologyProfile.Node {
		s.topologyFlisr.AddNode(node.Id, node.EquipmentId, node.EquipmentTypeId, node.EquipmentName)
	}

	for _, edge := range s.topologyProfile.Edge {
		if err := s.topologyFlisr.AddEdge(edge.Id, edge.Terminal1, edge.Terminal2, edge.StateNormal, edge.EquipmentId, edge.EquipmentTypeId, edge.EquipmentName); err != nil {
			return err
		}
	}

	s.topologyGrid = topogrid.New(len(s.topologyProfile.Node))

	for _, node := range s.topologyProfile.Node {
		s.topologyGrid.AddNode(node.Id, node.EquipmentId, node.EquipmentTypeId, node.EquipmentName)
	}

	for _, edge := range s.topologyProfile.Edge {
		if err := s.topologyGrid.AddEdge(edge.Id, edge.Terminal1, edge.Terminal2, edge.StateNormal, edge.EquipmentId, edge.EquipmentTypeId, edge.EquipmentName); err != nil {
			return err
		}
	}

	return nil
}

func (s *ThisService) ZmqReceiveDataHandler(msg []string) {
	for _, data := range msg {
		_message, err := types.ParseScadaRtdbData([]byte(data))
		if err != nil {
			s.log.Errorf("Failed to parse incoming data (%s): %v", data, err)
			continue
		}
		for _, point := range _message {
			if _, exists := s.resourceStructFromPointId[point.Id]; exists {
				s.inputDataQueue <- point
			}
		}
	}
}

// CurrentStateWorker calculates current topology and sends state of SLD elements
func (s *ThisService) CurrentStateWorker() {
	for point := range s.switchDataQueue {
		if resource, exists := s.resourceStructFromPointId[point.Id]; exists {
			if err := s.topologyGrid.SetSwitchStateByEquipmentId(resource.equipmentId, int(point.Value)); err != nil {
				s.log.Errorf("Failed to change state: %v", err)
				continue
			}

			s.topologyGrid.SetEquipmentElectricalState()

			for _, equipmentId := range s.equipmentIdArrayFromResourceTypeId[types.ResourceTypeStateLineSegment] {
				if newElectricalState, exists := s.topologyGrid.EquipmentElectricalStateByEquipmentId(equipmentId); exists {
					if pointId, exists := s.pointFromEquipmentIdAndResourceTypeId[equipmentId][types.ResourceTypeStateLineSegment]; exists {
						//s.log.Debugf("%d:%d->%d", equipmentId, newElectricalState, pointId)

						var electricalState float32 = ResourceStateIsolated

						if newElectricalState&topogrid.StateEnergized == topogrid.StateEnergized {
							electricalState = ResourceStateGood
						} else if newElectricalState&topogrid.StateIsolated == topogrid.StateIsolated {
							electricalState = ResourceStateIsolated
						} else if newElectricalState&topogrid.StateFault == topogrid.StateFault {
							electricalState = ResourceStateFault
						}

						s.outputEventQueue <- types.RtdbMessage{Id: pointId, Value: electricalState}
					}
				}
			}
		}
	}
}

func (s *ThisService) ReceiveDataWorker() {
	//var err error
	for point := range s.inputDataQueue {
		resource := s.resourceStructFromPointId[point.Id]

		//if equipment, exists := s.equipmentFromEquipmentId[resource.equipmentId]; !exists {
		//	continue
		//} else {
		//	if equipment.TypeId != topogrid.TypeCircuitBreaker &&
		//		equipment.TypeId != topogrid.TypeDisconnectSwitch {
		//		continue
		//	}
		//}

		switch resource.resourceTypeId {
		case types.ResourceTypeState:
			s.log.Debugf("State: %s", point)
			// For current state calculating
			s.switchDataQueue <- point

			s.stateSwitchBuffer = append(s.stateSwitchBuffer, point)

			_ = s.stateMachine.NextState(resource.resourceTypeId)
		case types.ResourceTypeProtect:
			if point.Value == 1 {
				s.log.Debugf("Protect: %s", point)
				s.alarmProtectBuffer = append(s.alarmProtectBuffer, point)
				_ = s.stateMachine.NextState(resource.resourceTypeId)
			}
		case types.ResourceTypeReclosing:
			if point.Value == 1 {
				s.log.Debugf("Reclosing: %s", point)
				_ = s.stateMachine.NextState(resource.resourceTypeId)
			}
			//default:
			//	s.log.Errorf("Unknown resource type: %s", point)
		}

		//if err != nil {
		//	s.log.Debugf("P:%d: %v", point.Id, err)
		//}
	}
}

func (s *ThisService) OutputEventWorker() {
	for event := range s.outputEventQueue {

		event.Timestamp.Time = time.Now()
		event.TimestampRecv.Time = time.Now()
		event.Quality = 0
		event.Source = s.config.pointSource

		// s.log.Debugf("O: %+v", event)

		data, err := json.Marshal([]types.RtdbMessage{event})
		if err != nil {
			s.log.Errorf("Failed to marshal (%+v): %v", event, err)
			continue
		}

		_, err = s.zmq.Send(s.rtdbPublisherIdx, data)
		if err != nil {
			s.log.Errorf("Failed to send event (%+v): %v", event, err)
			continue
		}
	}
}

func (s *ThisService) OutputMessageWorker() {
	for _message := range s.outputMessageQueue {

		s.log.Debugf("M: %+v", _message)

		data, err := json.Marshal([]message.OutputMessageStruct{_message})
		if err != nil {
			s.log.Errorf("Failed to marshal (%+v): %v", _message, err)
			continue
		}

		_, err = s.zmq.Send(s.messagePublisherIdx, data)
		if err != nil {
			s.log.Errorf("Failed to send message (%+v): %v", _message, err)
			continue
		}
	}
}

func (s *ThisService) FlisrGetFaultyEquipment() (int, int, int64) {
	equipmentArray := make([]int, 0)

	for _, event := range s.alarmProtectBuffer {
		resource := s.resourceStructFromPointId[event.Id]
		equipmentArray = append(equipmentArray, resource.equipmentId)
	}

	faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches := s.topologyFlisr.GetFurthestEquipmentFromPower(equipmentArray)
	s.log.Debugf("Fault in:%v->%d:%d:%d", equipmentArray, faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches)

	return faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches
}

func (s *ThisService) FlisrIsolateEquipment(faultyCBEquipmentId int, powerSupplyNodeId int) error {
	furthestTerminalNodeId := s.topologyFlisr.GetFurthestEquipmentNodeIdFromPower(powerSupplyNodeId, faultyCBEquipmentId)
	if furthestTerminalNodeId == 0 {
		return fmt.Errorf("furthest terminal node was not found for equipment id: %d", faultyCBEquipmentId)
	}

	s.log.Debugf("Energized by node:%d Equipment:%d:'%s'->Node:%d", powerSupplyNodeId, faultyCBEquipmentId, s.topologyFlisr.EquipmentNameByEquipmentId(faultyCBEquipmentId), furthestTerminalNodeId)

	cbList, faultyEquipments, err := s.topologyFlisr.GetCircuitBreakersEdgeIdsNextToNode(furthestTerminalNodeId)

	if err != nil {
		return err
	}

	s.SendFlisrAlarmForEquipments(faultyEquipments, types.ResourceTypeStateLineSegment, 2)
	s.SendFlisrMessageForEquipments(faultyEquipments, "alarm-yellow", "Авария")

	for _, cbId := range cbList {
		if equipmentId, err := s.topologyFlisr.EquipmentIdByEdgeId(cbId); err == nil {
			s.SendFlisrMessageForEquipment(equipmentId, "alarm-blue", "Отключить")
		}
	}

	// List of CBs to switch to OFF state
	s.log.Debugf("Need to OFF %+v:[%s]", cbList, s.topologyFlisr.EquipmentNameByEdgeIdArray(cbList))

	//for _, cbId := range cbList {
	//	equipmentId, err := s.topologyFlisr.EquipmentIdByEdgeId(cbId)
	//	if err != nil {
	//		return err
	//	}
	//
	//	err = s.topologyFlisr.SetSwitchStateByEquipmentId(equipmentId, 0)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (s *ThisService) SendFlisrMessageForEquipment(equipmentId int, class string, text string) {
	equipment := s.equipmentFromEquipmentId[equipmentId]
	_message := message.New()
	_message.CreateMessage(class, equipment.Name, text)
	s.outputMessageQueue <- *_message
}

func (s *ThisService) SendFlisrMessageForEquipments(equipments map[int]bool, class string, text string) {
	_message := message.New()

	for equipmentId := range equipments {
		equipment := s.equipmentFromEquipmentId[equipmentId]
		_message.CreateMessage(class, equipment.Name, text)
		s.outputMessageQueue <- *_message
	}
}

func (s *ThisService) SendFlisrAlarmForEquipment(equipmentId int, value int) {

	if pointId, exists := s.pointFromEquipmentIdAndResourceTypeId[equipmentId][types.ResourceTypeState]; exists {
		s.outputEventQueue <- types.RtdbMessage{Id: pointId, Value: float32(value)}
	}
}

func (s *ThisService) SendFlisrAlarmForEquipments(equipments map[int]bool, resourceType int, value int) {
	for equipmentId := range equipments {
		if pointId, exists := s.pointFromEquipmentIdAndResourceTypeId[equipmentId][resourceType]; exists {
			s.outputEventQueue <- types.RtdbMessage{Id: pointId, Value: float32(value)}
		}
	}
}

func (s *ThisService) StateHandler(state sm.StateStruct) {

	if state.OutMessage != "" {
		s.log.Debugf("%d->%s", state.ThisState, state.OutMessage)
	}

	if state.ThisState == sm.StateInit {
		s.alarmProtectBuffer = s.alarmProtectBuffer[:0]
		s.stateSwitchBuffer = s.stateSwitchBuffer[:0]
	}

	switch state.OutTag {
	case OutTagReclosingOk:
		s.log.Debugf("%d->%s: %+v", state.ThisState, state.OutMessage, s.alarmProtectBuffer)
	case OutTagReclosingFailed:
		s.log.Debugf("%d->%s: %+v", state.ThisState, state.OutMessage, s.alarmProtectBuffer)

		faultyCBEquipmentId, powerSupplyNodeId, _ := s.FlisrGetFaultyEquipment()

		if faultyCBEquipmentId != 0 {
			err := s.FlisrIsolateEquipment(faultyCBEquipmentId, powerSupplyNodeId)
			if err != nil {
				s.log.Errorf("Failed to isolate equipment id %d: %v", faultyCBEquipmentId, err)
			}
		} else {
			s.log.Errorf("Failed to find faulty equipment")
		}
	}

}

func (s *ThisService) InitStateMachine() error {
	return s.stateMachine.LoadConfiguration(s.config.stateMachineConfig)
}

func main() {
	var err error
	var configFile string
	var isLoadFromCache bool

	flag.StringVar(&configFile, "conf", "flisr.cfg", "path to FLISR configuration file")
	flag.BoolVar(&isLoadFromCache, "cache", false, "load profile from local cache")
	flag.Parse()

	s := New()

	if err = s.ReadConfig(configFile); err != nil {
		s.log.Fatalf("Failed to read configuration %s: %v", configFile, err)
	}

	s.stateMachine = sm.New(s.log, s.StateHandler)
	s.outputEventQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.inputDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.switchDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.outputMessageQueue = make(chan message.OutputMessageStruct, s.config.jobQueueLength)

	var logLevel llog.Level

	if logLevel, err = llog.ParseLevel(s.config.logLevel, llog.WarnLevel); err != nil {
		s.log.Errorf("Failed to parse log level: %v", err)
	}

	s.log.SetLevel(logLevel)

	s.log.Infof("Log level: %s", s.log.GetLevel().UpperString())

	if err = s.LoadTopologyProfile(time.Second*ApiTimeoutSec, isLoadFromCache, "cache/flisr-topology.json"); err != nil {
		s.log.Fatalf("Failed to load topology profile: %v", err)
	}

	if err = s.LoadEquipmentProfile(time.Second*ApiTimeoutSec, isLoadFromCache, "cache/flisr-equipment.json"); err != nil {
		s.log.Fatalf("Failed to load equipment profile: %v", err)
	}

	s.CreateInternalParametersFromProfiles()

	if err = s.LoadTopologyGrid(); err != nil {
		s.log.Fatalf("Failed to load topology: %v", err)
	}

	if err = s.InitStateMachine(); err != nil {
		s.log.Fatalf("Failed to load state machine configuration: %v", err)
	}

	if s.zmq, err = zmq_bus.New(1, 2); err != nil {
		s.log.Fatalf("Failed to create zmq context: %v", err)
	}

	var subscriberIdx int
	if subscriberIdx, err = s.zmq.AddSubscriber(s.config.zmqRtdb); err != nil {
		s.log.Fatalf("Failed to add zmq subscriber [%s]: %v", s.config.zmqRtdb, err)
	}

	s.zmq.SetReceiveHandler(subscriberIdx, s.ZmqReceiveDataHandler)

	if s.rtdbPublisherIdx, err = s.zmq.AddPublisher(s.config.zmqRtdbInput); err != nil {
		s.log.Fatalf("Failed to add zmq event publisher [%s]: %v", s.config.zmqRtdbInput, err)
	}

	if s.messagePublisherIdx, err = s.zmq.AddPublisher(s.config.zmqAlarmMessage); err != nil {
		s.log.Fatalf("Failed to add zmq message publisher [%s]: %v", s.config.zmqAlarmMessage, err)
	}

	go s.ReceiveDataWorker()
	go s.OutputMessageWorker()
	go s.OutputEventWorker()

	s.log.Infof("Started")

	s.topologyFlisr.SetEquipmentElectricalState()

	//fmt.Printf("%s\n", s.stateMachine.GetAsGraphMl())

	go s.CurrentStateWorker()

	err = s.zmq.WaitingLoop()

	s.log.Errorf("Stopped: %v", err)
}
