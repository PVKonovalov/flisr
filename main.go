package main

import (
	"encoding/json"
	"errors"
	"flag"
	"flisr/llog"
	"flisr/message"
	sm "flisr/state_machine"
	"flisr/types"
	"flisr/webapi"
	"flisr/zmq_bus"
	"fmt"
	"github.com/PVKonovalov/localcache"
	"github.com/PVKonovalov/topogrid"
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
	MessageFault     = "Fault"
	MessageIsolate   = "Isolate"
	MessageSwitchOn  = "Switch On"
	MessagePowerFrom = "Power from"
)
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

// Resource Types
const (
	ResourceTypeIsNotDefine      int = 0
	ResourceTypeMeasure          int = 1
	ResourceTypeState            int = 2
	ResourceTypeControl          int = 3
	ResourceTypeProtection       int = 4
	ResourceTypeLink             int = 5
	ResourceTypeChangeSetGroup   int = 6
	ResourceTypeReclosing        int = 7
	ResourceTypeStateLineSegment int = 8
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
	flisrStateRtdbPoint      uint64
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
	pointNameFromPointId                  map[uint64]string
	zmq                                   *zmq_bus.ZmqBus
	outputEventQueue                      chan types.RtdbMessage
	inputDataQueue                        chan types.RtdbMessage
	switchDataQueue                       chan types.RtdbMessage
	linkStateDataQueue                    chan types.RtdbMessage
	outputMessageQueue                    chan message.OutputMessageStruct
	stateMachine                          *sm.StateMachine
	alarmProtectBuffer                    []types.RtdbMessage
	stateSwitchBuffer                     []types.RtdbMessage
	rtdbPublisherIdx                      int
	messagePublisherIdx                   int
	numberOfCBCheckingLink                int
	enableAutoMode                        bool
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
		pointNameFromPointId:                  make(map[uint64]string),
		enableAutoMode:                        false,
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

	configAPIHostList := cfg.Section("FLISR").Key("HOST").String()
	if configAPIHostList == "" {
		return fmt.Errorf("FLISR/HOST was not found")
	} else {
		s.config.configAPIHostList = strings.Split(configAPIHostList, ",")
		if len(s.config.configAPIHostList[0]) == 0 {
			return fmt.Errorf("FLISR/HOST was not found")
		}
	}

	s.config.zmqRtdb = cfg.Section("BUSES").Key("ZMQ_RTDB_OUTPUT_POINT").String()
	s.config.zmqRtdbCommand = cfg.Section("BUSES").Key("ZMQ_RTDB_COMMAND_POINT").String()
	s.config.zmqRtdbInput = cfg.Section("BUSES").Key("ZMQ_RTDB_INPUT_POINT").String()

	s.config.zmqAlarmMessage = cfg.Section("SLD_BRIDGE").Key("ZMQ_ALARM_MESSAGE_POINT").String()
	if s.config.zmqAlarmMessage == "" {
		return fmt.Errorf("SLD_BRIDGE/ZMQ_ALARM_MESSAGE_POINT is not found")
	}

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

	if s.config.flisrStateRtdbPoint, err = cfg.Section("FLISR").Key("RTDB_POINT_FLISR_STATE").Uint64(); err != nil {
		s.config.flisrStateRtdbPoint = 0
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
			if resource.TypeId == ResourceTypeProtection ||
				resource.TypeId == ResourceTypeReclosing ||
				resource.TypeId == ResourceTypeState ||
				resource.TypeId == ResourceTypeStateLineSegment ||
				resource.TypeId == ResourceTypeLink {

				s.pointNameFromPointId[resource.PointId] = resource.Point

				s.resourceStructFromPointId[resource.PointId] = ResourceStruct{
					equipmentId:    equipment.Id,
					resourceTypeId: resource.TypeId,
				}

				if _, exists := s.pointFromEquipmentIdAndResourceTypeId[equipment.Id]; !exists {
					s.pointFromEquipmentIdAndResourceTypeId[equipment.Id] = make(map[int]uint64)
				}
				s.pointFromEquipmentIdAndResourceTypeId[equipment.Id][resource.TypeId] = resource.PointId

				if resource.TypeId == ResourceTypeLink {
					s.numberOfCBCheckingLink += 1
				}
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

			for _, equipmentId := range s.equipmentIdArrayFromResourceTypeId[ResourceTypeStateLineSegment] {
				if newElectricalState, exists := s.topologyGrid.EquipmentElectricalStateByEquipmentId(equipmentId); exists {
					if pointId, exists := s.pointFromEquipmentIdAndResourceTypeId[equipmentId][ResourceTypeStateLineSegment]; exists {
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
	var condition sm.Condition
	for point := range s.inputDataQueue {
		resource := s.resourceStructFromPointId[point.Id]

		switch resource.resourceTypeId {
		case ResourceTypeState:
			s.switchDataQueue <- point

			if point.Value == 1 {
				condition = sm.ConditionSwitchOn
			} else {
				condition = sm.ConditionSwitchOff
			}

			s.log.Debugf("%3s: %s %s", condition.Name(), s.pointNameFromPointId[point.Id], point)

			s.stateSwitchBuffer = append(s.stateSwitchBuffer, point)

			_ = s.stateMachine.NextState(condition)
		case ResourceTypeProtection:
			condition = sm.ConditionProtect

			if point.Value == 1 {
				s.log.Debugf("%3s: %s %s", condition.Name(), s.pointNameFromPointId[point.Id], point)
				s.alarmProtectBuffer = append(s.alarmProtectBuffer, point)
				_ = s.stateMachine.NextState(condition)
			}
		case ResourceTypeReclosing:
			condition = sm.ConditionReclosing

			if point.Value == 1 {
				s.log.Debugf("%3s: %s %s", condition.Name(), s.pointNameFromPointId[point.Id], point)
				_ = s.stateMachine.NextState(condition)
			}

		case ResourceTypeLink:
			s.linkStateDataQueue <- point
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
		event.TimestampFromClient = 0

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

	s.topologyFlisr.PrintfEquipments(topogrid.TypeCircuitBreaker)

	faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches := s.topologyFlisr.GetFurthestEquipmentFromPower(equipmentArray)
	s.log.Debugf("Fault in:%v->%d:%d:%d", equipmentArray, faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches)

	return faultyCBEquipmentId, powerSupplyNodeId, numberOfSwitches
}

func (s *ThisService) GetListCbToIsolateSegmentAndFaultyEquipment(faultyCBEquipmentId int, powerSupplyNodeId int) ([]int, map[int]bool, error) {

	furthestTerminalNodeId := s.topologyFlisr.GetFurthestEquipmentTerminalIdFromPower(powerSupplyNodeId, faultyCBEquipmentId)

	if furthestTerminalNodeId == 0 {
		return nil, nil, fmt.Errorf("furthest terminal node was not found for equipment id: %d", faultyCBEquipmentId)
	}

	s.log.Debugf("Energized by node:%d Equipment:%d:'%s'->Node:%d", powerSupplyNodeId, faultyCBEquipmentId, s.topologyFlisr.EquipmentNameByEquipmentId(faultyCBEquipmentId), furthestTerminalNodeId)

	cbList, faultyEquipments, err := s.topologyFlisr.GetCircuitBreakersEdgeIdsNextToNode(furthestTerminalNodeId)

	if err != nil {
		return nil, nil, err
	}

	return cbList, faultyEquipments, nil
}

func (s *ThisService) GetListCbToRestoreService(listCbToTurnOff []int, faultyEquipments map[int]bool) (map[int]map[int][]int, error) {
	var err error

	mapCbToTurnOn := make(map[int]map[int][]int) // [wantToRestoreEquipmentId][poweredFromEquipmentId][]CbToOn
	mapCbToTurnOff := make(map[int]bool)

	for _, edgeId := range listCbToTurnOff {
		if equipmentCbId, err1 := s.topologyFlisr.EquipmentIdByEdgeId(edgeId); err1 == nil {
			mapCbToTurnOff[equipmentCbId] = true
		}
	}

	if err = s.UpdateTopologyByListCb(listCbToTurnOff, 0); err != nil {
		s.log.Errorf("Failed to update topology: %v", err)
	}

	s.topologyFlisr.SetEquipmentElectricalState()

	for _, wantToRestoreEquipmentId := range s.equipmentIdArrayFromResourceTypeId[ResourceTypeStateLineSegment] {
		if newElectricalState, exists := s.topologyGrid.EquipmentElectricalStateByEquipmentId(wantToRestoreEquipmentId); exists {
			if _, exists = faultyEquipments[wantToRestoreEquipmentId]; !exists {
				if newElectricalState == topogrid.StateIsolated {
					equipment := s.equipmentFromEquipmentId[wantToRestoreEquipmentId]
					cbListToEnergizeEquipment := s.topologyFlisr.GetCbListToEnergizeEquipment(wantToRestoreEquipmentId)

					for powerEquipmentId, cbList := range cbListToEnergizeEquipment {
						s.log.Debugf("Restore: %s <- %s[%s]", equipment.Name, s.topologyFlisr.EquipmentNameByEquipmentId(powerEquipmentId), s.topologyFlisr.EquipmentNameByEquipmentIdArray(cbList))

						cbListOn := make([]int, 0)

						for _, cbId := range cbList {
							if _, exists1 := mapCbToTurnOff[cbId]; exists1 {
								cbListOn = cbListOn[:0]
								break
							}

							if cbSwitchState, exists1 := s.topologyFlisr.EquipmentSwitchStateByEquipmentId(cbId); exists1 && cbSwitchState == 0 {
								if _, violatedCondition := s.topologyFlisr.CanBeSwitchedOn(cbId); violatedCondition != nil {
									s.log.Debugf("don't turn on switch %s: %v", s.topologyFlisr.EquipmentNameByEquipmentId(cbId), violatedCondition)
									cbListOn = cbListOn[:0]
								} else {
									cbListOn = append(cbListOn, cbId)
								}
							}
						}

						if len(cbListOn) != 0 {
							if mapCbToTurnOn[wantToRestoreEquipmentId] == nil {
								mapCbToTurnOn[wantToRestoreEquipmentId] = make(map[int][]int)
							}

							mapCbToTurnOn[wantToRestoreEquipmentId][powerEquipmentId] = cbListOn
							s.log.Debugf("%s:[%s]", s.topologyFlisr.EquipmentNameByEquipmentId(powerEquipmentId), s.topologyFlisr.EquipmentNameByEquipmentIdArray(cbListOn))
						}
					}

				}
			}
		}
	}

	return mapCbToTurnOn, nil
}

func (s *ThisService) FlisrIsolate(listCbToTurnOff []int) {
	s.log.Debugf("Auto isolate")
}

func (s *ThisService) FlisrRestoreService(listCbToTurnOn []int) {
	s.log.Debugf("Auto restore service")
}

func (s *ThisService) UpdateTopologyByListCb(listCb []int, switchState int) error {

	var err error
	var equipmentId int

	for _, edgeId := range listCb {
		if equipmentId, err = s.topologyFlisr.EquipmentIdByEdgeId(edgeId); err == nil {
			if err = s.topologyFlisr.SetSwitchStateByEquipmentId(equipmentId, switchState); err != nil {
				return err
			}
		}
	}
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

	if pointId, exists := s.pointFromEquipmentIdAndResourceTypeId[equipmentId][ResourceTypeState]; exists {
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

func (s *ThisService) FromStateHandler(state sm.StateStruct) {
	if state.ThisState == sm.StateInit {
		if err := s.topologyFlisr.CopyEquipmentSwitchStateFrom(s.topologyGrid); err != nil {
			s.log.Errorf("Update topology: %v", err)
		}
	}
}

func (s *ThisService) ToStateHandler(state sm.StateStruct) {

	if state.OutMessage != "" {
		s.log.Debugf("%d->%s", state.ThisState, state.OutMessage)
	}

	if state.ThisState == sm.StateInit {
		s.alarmProtectBuffer = s.alarmProtectBuffer[:0]
		s.stateSwitchBuffer = s.stateSwitchBuffer[:0]
		s.log.Debugf("INIT")
	}

	switch state.OutTag {
	case OutTagReclosingOk:
		s.log.Debugf("%d->%s: %+v", state.ThisState, state.OutMessage, s.alarmProtectBuffer)
	case OutTagReclosingFailed:
		s.log.Debugf("%d->%s: %+v", state.ThisState, state.OutMessage, s.alarmProtectBuffer)

		faultyCBEquipmentId, powerSupplyNodeId, _ := s.FlisrGetFaultyEquipment()

		if faultyCBEquipmentId != 0 {
			if listCbToTurnOff, faultyEquipments, err := s.GetListCbToIsolateSegmentAndFaultyEquipment(faultyCBEquipmentId, powerSupplyNodeId); err == nil {
				s.log.Debugf("Turn off CB: [%s]", s.topologyFlisr.EquipmentNameByEdgeIdArray(listCbToTurnOff))

				s.SendFlisrAlarmForEquipments(faultyEquipments, ResourceTypeStateLineSegment, 2)
				s.SendFlisrMessageForEquipments(faultyEquipments, "alarm-yellow", MessageFault)

				for _, cbId := range listCbToTurnOff {
					if equipmentId, err := s.topologyFlisr.EquipmentIdByEdgeId(cbId); err == nil {
						s.SendFlisrMessageForEquipment(equipmentId, "alarm-blue", MessageIsolate)
					}
				}

				if s.enableAutoMode {
					s.FlisrIsolate(listCbToTurnOff)
				}

				if errCopy := s.topologyFlisr.CopyEquipmentSwitchStateFrom(s.topologyGrid); errCopy != nil {
					s.log.Errorf("Update topology: %v", errCopy)
				}

				s.topologyFlisr.SetEquipmentElectricalState()

				if mapCbToTurnOn, err := s.GetListCbToRestoreService(listCbToTurnOff, faultyEquipments); err == nil {
					for restoreEquipmentId, mapPoweredByEquipment := range mapCbToTurnOn {
						for poweredByEquipment, listCbToTurnOn := range mapPoweredByEquipment {
							if len(listCbToTurnOn) != 0 {
								s.SendFlisrMessageForEquipment(restoreEquipmentId, "alarm-blue", MessagePowerFrom+" "+s.equipmentFromEquipmentId[poweredByEquipment].Name)
							}
							for _, equipmentId := range listCbToTurnOn {
								s.SendFlisrMessageForEquipment(equipmentId, "alarm-blue", MessageSwitchOn)
							}

							if s.enableAutoMode {
								s.FlisrRestoreService(listCbToTurnOn)
							}
						}
					}
				}
			} else {
				s.log.Errorf("Failed to isolate equipment %d: %v", faultyCBEquipmentId, err)
			}
		} else {
			s.log.Errorf("Failed to find faulty equipment")
		}
	}
}

func (s *ThisService) InitStateMachine() error {
	return s.stateMachine.LoadConfiguration(s.config.stateMachineConfig)
}

func (s *ThisService) CBLinkCheckWorker() {
	linkStateCB := make(map[int]bool)
	rtdbMessage := types.RtdbMessage{Id: s.config.flisrStateRtdbPoint}
	currentValue := float32(-1)

	for linkSate := range s.linkStateDataQueue {
		if resource, exists := s.resourceStructFromPointId[linkSate.Id]; exists {
			if linkSate.Value == 1 {
				linkStateCB[resource.equipmentId] = true
			} else {
				delete(linkStateCB, resource.equipmentId)
			}
			if len(linkStateCB) == s.numberOfCBCheckingLink {
				rtdbMessage.Value = 1
			} else {
				rtdbMessage.Value = 0
			}

			if rtdbMessage.Value != currentValue {
				currentValue = rtdbMessage.Value
				s.outputEventQueue <- rtdbMessage
			}
		}
	}
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

	s.stateMachine = sm.New(s.log, s.FromStateHandler, s.ToStateHandler)
	s.outputEventQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.inputDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.switchDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.outputMessageQueue = make(chan message.OutputMessageStruct, s.config.jobQueueLength)
	s.linkStateDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)

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
	go s.CBLinkCheckWorker()

	s.log.Infof("Started")

	s.topologyFlisr.SetEquipmentElectricalState()

	//fmt.Printf("%s\n", s.stateMachine.GetAsGraphMl())
	// fmt.Printf("%s\n", s.topologyFlisr.GetAsGraphMl())

	go s.CurrentStateWorker()

	s.stateMachine.Start()

	err = s.zmq.WaitingLoop()

	s.log.Errorf("Stopped: %v", err)
}
