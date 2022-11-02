package main

import (
	"encoding/json"
	"errors"
	"flag"
	"flisr/llog"
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

// Resource Types
const (
	ResourceTypeMeasure          = 1
	ResourceTypeState            = 2
	ResourceTypeControl          = 3
	ResourceTypeProtect          = 4
	ResourceTypeLink             = 5
	ResourceTypeChangeSetGroup   = 6
	ResourceTypeReclosing        = 7
	ResourceTypeStateLineSegment = 8
)

// ConfigStruct Structure with current Scada configuration
type ConfigStruct struct {
	username                 string
	password                 string
	configAPIHostVirtualName string
	configAPIHostList        []string
	logLevel                 string
	zmqRtdb                  string
	zmqRtdbCommand           string
	zmqRtdbInput             string
	jobQueueLength           int
	acrDelaySec              int
	rtdbPointFlisrState      uint64
	pointSource              uint32
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
	log                       *llog.LevelLog
	config                    ConfigStruct
	topologyProfile           *TopologyStruct
	equipmentFromEquipmentId  map[int]EquipmentStruct
	topology                  *topogrid.TopologyGridStruct
	resourceStructFromPointId map[uint64]ResourceStruct
	zmq                       *zmq_bus.ZmqBus
	outputQueue               chan types.RtdbMessage
	inputDataQueue            chan types.RtdbMessage
}

// New service
func New() *ThisService {
	return &ThisService{
		log:                       llog.NewLevelLog(llog.Ldate | llog.Ltime | llog.Lmicroseconds),
		equipmentFromEquipmentId:  make(map[int]EquipmentStruct),
		resourceStructFromPointId: make(map[uint64]ResourceStruct),
	}
}

// ReadConfig from configFile path
func (s *ThisService) ReadConfig(configFile string) error {
	cfg, err := ini.Load(configFile)
	if err != nil {
		return err
	}

	s.config.username = cfg.Section("CONFIGAPI").Key("USERNAME").String()
	s.config.password = cfg.Section("CONFIGAPI").Key("PASSWORD").String()
	s.config.configAPIHostVirtualName = cfg.Section("CONFIGAPI").Key("HOST_VIRTUAL_NAME").String()
	s.config.configAPIHostList = strings.Split(cfg.Section("CONFIGAPI").Key("HOST").String(), ",")

	s.config.zmqRtdb = cfg.Section("BUSES").Key("ZMQ_RTDB_OUTPUT_POINT").String()
	s.config.zmqRtdbCommand = cfg.Section("BUSES").Key("ZMQ_RTDB_COMMAND_POINT").String()
	s.config.zmqRtdbInput = cfg.Section("BUSES").Key("ZMQ_RTDB_INPUT_POINT").String()

	s.config.logLevel = cfg.Section("FLISR").Key("LOG").String()
	s.config.jobQueueLength, err = cfg.Section("FLISR").Key("QUEUE").Int()
	if err != nil {
		s.config.jobQueueLength = 100
	}

	s.config.acrDelaySec, err = cfg.Section("FLISR").Key("ACR_DELAY_SEC").Int()
	if err != nil {
		s.config.acrDelaySec = 5
	}

	var pointSource uint64

	pointSource, err = cfg.Section("FLISR").Key("POINT_SOURCE").Uint64()
	s.config.pointSource = uint32(pointSource)
	if err != nil {
		s.config.pointSource = 0
	}

	s.config.rtdbPointFlisrState, err = cfg.Section("FLISR").Key("RTDB_POINT_FLISR_STATE").Uint64()
	if err != nil {
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

// LoadTopology Loading topologyProfile from configs.configAPIHostList
func (s *ThisService) LoadTopology(timeoutSec time.Duration, isLoadFromCache bool, cachePath string) error {
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

// LoadEquipment Loading equipment from config.ConfigAPIHostList
func (s *ThisService) LoadEquipment(timeoutSec time.Duration, isLoadFromCache bool, cachePath string) error {
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

func (s *ThisService) CreateInternalParametersFromProfile() {
	for _, equipment := range s.equipmentFromEquipmentId {
		for _, resource := range equipment.Resource {
			if resource.TypeId == ResourceTypeProtect ||
				resource.TypeId == ResourceTypeReclosing ||
				resource.TypeId == ResourceTypeStateLineSegment {
				s.resourceStructFromPointId[resource.PointId] = ResourceStruct{
					equipmentId:    equipment.Id,
					resourceTypeId: resource.TypeId,
				}
			}
		}
	}
}

func (s *ThisService) LoadTopologyGrid() error {
	s.topology = topogrid.New(len(s.topologyProfile.Node))

	for _, node := range s.topologyProfile.Node {
		s.topology.AddNode(node.Id, node.EquipmentId, node.EquipmentTypeId, node.EquipmentName)
	}

	for _, edge := range s.topologyProfile.Edge {
		err := s.topology.AddEdge(edge.Id, edge.Terminal1, edge.Terminal2, edge.StateNormal, edge.EquipmentId, edge.EquipmentTypeId, edge.EquipmentName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ThisService) ZmqReceiveDataHandler(msg []string) {
	for _, data := range msg {
		message, err := types.ParseScadaRtdbData([]byte(data))
		if err != nil {
			s.log.Errorf("Failed to parse incoming data (%s): %v", data, err)
			continue
		}
		for _, point := range message {
			if _, exists := s.resourceStructFromPointId[point.Id]; exists {
				s.inputDataQueue <- point
			}
		}
	}
}

func (s *ThisService) ReceiveDataWorker() {
	for point := range s.inputDataQueue {
		s.log.Debugf("D: %+v", point)
	}
}

func (s *ThisService) OutputMessageWorker() {
	for message := range s.outputQueue {
		s.log.Debugf("O: %+v", message)

		data, err := json.Marshal([]types.RtdbMessage{message})
		if err != nil {
			s.log.Fatalf("Failed to marshal (%+v): %v", message, err)
		}

		_, err = s.zmq.Send(0, data)
		if err != nil {
			s.log.Fatalf("Failed to send message (%+v): %v", message, err)
		}
	}
}

func main() {
	s := New()

	var configFile string
	var isLoadFromCache bool

	flag.StringVar(&configFile, "conf", "flisr.cfg", "path to FLISR configuration file")
	flag.BoolVar(&isLoadFromCache, "cache", false, "load profile from local cache")
	flag.Parse()

	err := s.ReadConfig(configFile)
	if err != nil {
		s.log.Fatalf("Failed to read configuration %s: %v", configFile, err)
	}

	s.outputQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)
	s.inputDataQueue = make(chan types.RtdbMessage, s.config.jobQueueLength)

	logLevel, err := llog.ParseLevel(s.config.logLevel)
	if err != nil {
		s.log.Errorf("Failed to parse log level (%s): %v", s.config.logLevel, err)
		s.log.SetLevel(llog.WarnLevel)
	} else {
		s.log.SetLevel(logLevel)
	}

	s.log.Infof("Log level: %s", s.log.GetLevel().UpperString())

	err = s.LoadTopology(time.Second*180, isLoadFromCache, "cache/flisr-topology.json")

	if err != nil {
		s.log.Fatalf("Failed to load topology profile: %v", err)
	}

	err = s.LoadEquipment(time.Second*180, isLoadFromCache, "cache/flisr-equipment.json")

	if err != nil {
		s.log.Fatalf("Failed to load equipment profile: %v", err)
	}

	s.CreateInternalParametersFromProfile()

	err = s.LoadTopologyGrid()

	if err != nil {
		s.log.Fatalf("Failed to load topology: %v", err)
	}

	s.zmq, err = zmq_bus.New(1, 1)

	if err != nil {
		s.log.Fatalf("Failed to create zmq context: %v", err)
	}

	idx, err := s.zmq.AddSubscriber(s.config.zmqRtdb)

	if err != nil {
		s.log.Fatalf("Failed to add zmq subscriber (%s): %v", s.config.zmqRtdb, err)
	}

	s.zmq.SetReceiveHandler(idx, s.ZmqReceiveDataHandler)

	_, err = s.zmq.AddPublisher(s.config.zmqRtdbInput)

	if err != nil {
		s.log.Fatalf("Failed to add zmq publisher [%s]: %v", s.config.zmqRtdbInput, err)
	}

	go s.ReceiveDataWorker()

	//s.log.Debugf("---- NodeIsPoweredBy ----")
	//for _, node := range s.topologyProfile.Node {
	//	poweredBy, err := s.topology.NodeIsPoweredBy(node.Id)
	//	if err != nil {
	//		s.log.Errorf("%v", err)
	//	}
	//	s.log.Debugf("%d:%s <- %v:%s", node.Id, s.topology.EquipmentNameByNodeId(node.Id), poweredBy, s.topology.EquipmentNameByNodeIdArray(poweredBy))
	//}
	//
	//s.log.Debugf("---- NodeCanBePoweredBy ----")
	//for _, node := range s.topologyProfile.Node {
	//	poweredBy, err := s.topology.NodeCanBePoweredBy(node.Id)
	//	if err != nil {
	//		s.log.Errorf("%v", err)
	//	}
	//	s.log.Debugf("%d:%s <- %v:%s", node.Id, s.topology.EquipmentNameByNodeId(node.Id), poweredBy, s.topology.EquipmentNameByNodeIdArray(poweredBy))
	//}
	//
	//s.log.Debugf("---- CircuitBreakersNextToNode ----")
	//for _, node := range s.topologyProfile.Node {
	//	poweredBy, err := s.topology.CircuitBreakersNextToNode(node.Id)
	//	if err != nil {
	//		s.log.Errorf("%v", err)
	//	}
	//	s.log.Debugf("%d:%s <- %v:%s", node.Id, s.topology.EquipmentNameByNodeId(node.Id), poweredBy, s.topology.EquipmentNameByEdgeIdArray(poweredBy))
	//}

	// fmt.Printf("%s", s.topology.GetAsGraphMl())

	s.topology.SetEquipmentElectricalState()
	// s.topology.PrintEquipment()

	// List of alarms and events
	events := []uint64{2649, 2599, 2588, 2663, 2674}
	equipmentArray := make([]int, 0)

	// List of alarms
	for _, event := range events {
		resource := s.resourceStructFromPointId[event]
		if resource.resourceTypeId == 4 {
			equipmentArray = append(equipmentArray, resource.equipmentId)
		}
	}

	equipmentId, powerSupplyNodeId, numberOfSwitches := s.topology.GetFurthestEquipmentFromPower(equipmentArray)
	fmt.Printf("%v->%d:%d:%d\n", equipmentArray, equipmentId, powerSupplyNodeId, numberOfSwitches)

	furthestNodeId := s.topology.GetFurthestEquipmentNodeIdFromPower(powerSupplyNodeId, equipmentId)
	if furthestNodeId == 0 {
		fmt.Printf("FurthestEquipmentNode is not found\n")
		return
	}

	fmt.Printf("%d:%d:'%s'->%d\n", powerSupplyNodeId, equipmentId, s.topology.EquipmentNameByEquipmentId(equipmentId), furthestNodeId)

	cbList, err := s.topology.GetCircuitBreakersEdgeIdsNextToNode(furthestNodeId)

	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	// List of CBs to switch to OFF state
	fmt.Printf("%+v:[%s]\n", cbList, s.topology.EquipmentNameByEdgeIdArray(cbList))

	for _, cbId := range cbList {
		equipmentId, err := s.topology.EquipmentIdByEdgeId(cbId)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}

		err = s.topology.SetSwitchStateByEquipmentId(equipmentId, 0)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
	}

	s.topology.SetEquipmentElectricalState()
	// s.topology.PrintEquipment()

}
