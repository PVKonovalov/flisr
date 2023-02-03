// Package topogrid contains implementations of basic power grid algorithms based on the grid topology.
//

package topogrid

import (
	"errors"
	"fmt"
	"github.com/yourbasic/graph"
	"sync"
)

const (
	SwitchStateOpen  = 0
	SwitchStateClose = 1
)

var ErrBothAreEnergized = errors.New("both segments are already energized")
var ErrEnergizedWillBeGrounded = errors.New("energized segment will be grounded")
var ErrSwitchIsAlreadyClosed = errors.New("switch is already closed")
var ErrEquipmentNotFound = errors.New("equipment not found")

type EquipmentStruct struct {
	id              int
	typeId          int
	name            string
	electricalState uint8
	poweredBy       map[int]int64
	switchState     int
}

type NodeStruct struct {
	idx             int
	id              int
	equipmentId     int
	electricalState uint8
}

type TerminalStruct struct {
	node1Id          int
	node2Id          int
	numberOfSwitches int64
}

type EdgeStruct struct {
	idx         int
	id          int
	equipmentId int
	terminal    TerminalStruct
}

type TopologyGridStruct struct {
	sync.RWMutex

	currentGraph *graph.Mutable // Current grid topology (depends on circuit breaker states)
	fullGraph    *graph.Mutable // Full grid topology

	nodes     []NodeStruct
	edges     []EdgeStruct
	equipment map[int]EquipmentStruct

	nodeIdxFromNodeId              map[int]int   // NodeId -> NodeIdx
	nodeIdArrayFromEquipmentTypeId map[int][]int // EquipmentTypeId -> []NodeId
	nodeIdArrayFromEquipmentId     map[int][]int // EquipmentId -> []NodeId

	edgeIdxFromEdgeId              map[int]int              // EdgeId -> EdgeIdx
	edgeIdArrayFromEquipmentTypeId map[int][]int            // EquipmentTypeId -> []EdgeId
	edgeIdArrayFromTerminalStruct  map[TerminalStruct][]int // TerminalStruct -> []EdgeId
	edgeIdArrayFromNodeId          map[int][]int            // NodeId -> []EdgeId
	edgeIdArrayFromEquipmentId     map[int][]int            // EquipmentId -> []EdgeId
	nodeIdx                        int
	edgeIdx                        int
}

// New topology
func New(numberOfNodes int) *TopologyGridStruct {
	return &TopologyGridStruct{
		currentGraph:                   graph.New(numberOfNodes),
		fullGraph:                      graph.New(numberOfNodes),
		nodes:                          make([]NodeStruct, numberOfNodes),
		nodeIdxFromNodeId:              make(map[int]int),
		nodeIdArrayFromEquipmentTypeId: make(map[int][]int),
		nodeIdArrayFromEquipmentId:     make(map[int][]int),
		edgeIdArrayFromEquipmentTypeId: make(map[int][]int),
		edgeIdxFromEdgeId:              make(map[int]int),
		edgeIdArrayFromTerminalStruct:  make(map[TerminalStruct][]int),
		edgeIdArrayFromNodeId:          make(map[int][]int),
		edgeIdArrayFromEquipmentId:     make(map[int][]int),
		edges:                          make([]EdgeStruct, 0),
		nodeIdx:                        0,
		edgeIdx:                        0,
		equipment:                      make(map[int]EquipmentStruct),
	}
}

// EquipmentNameByEquipmentId returns a string with node name from the equipment id
func (t *TopologyGridStruct) EquipmentNameByEquipmentId(equipmentId int) string {
	return t.equipment[equipmentId].name
}

// EquipmentNameByEquipmentIdArray returns a string with node name from the equipment id
func (t *TopologyGridStruct) EquipmentNameByEquipmentIdArray(equipmentIdArray []int) string {
	var name string
	for i, equipmentId := range equipmentIdArray {
		if i != 0 {
			name += ","
		}
		name += t.equipment[equipmentId].name
	}
	return name
}

// EquipmentNameByNodeIdx returns a string with node name from the node index
func (t *TopologyGridStruct) EquipmentNameByNodeIdx(idx int) string {
	return t.equipment[t.nodes[idx].equipmentId].name
}

// EquipmentNameByNodeId returns a string with node name from the node id
func (t *TopologyGridStruct) EquipmentNameByNodeId(id int) string {
	if idx, exists := t.nodeIdxFromNodeId[id]; exists {
		return t.EquipmentNameByNodeIdx(idx)
	} else {
		return ""
	}
}

// EquipmentNameByNodeIdArray returns a string with node names separated by ',' from an array of node ids
func (t *TopologyGridStruct) EquipmentNameByNodeIdArray(idArray []int) string {
	var name string
	for i, id := range idArray {
		if i != 0 {
			name += ","
		}
		name += t.EquipmentNameByNodeId(id)
	}
	return name
}

func (t *TopologyGridStruct) EquipmentNameByNodeIdxArray(idxArray []int) string {
	var name string
	for i, idx := range idxArray {
		if i != 0 {
			name += ","
		}
		name += t.equipment[t.nodes[idx].equipmentId].name
	}
	return name
}

// EquipmentNameByEdgeIdx returns a string with node name by the node index
func (t *TopologyGridStruct) EquipmentNameByEdgeIdx(idx int) string {
	return t.equipment[t.edges[idx].equipmentId].name
}

// EquipmentElectricalStateByEquipmentId returns an equipment electrical state by the equipment id
func (t *TopologyGridStruct) EquipmentElectricalStateByEquipmentId(id int) (uint8, bool) {
	equipment, exists := t.equipment[id]
	return equipment.electricalState, exists
}

func (t *TopologyGridStruct) EquipmentSwitchStateByEquipmentId(id int) (int, bool) {
	equipment, exists := t.equipment[id]
	return equipment.switchState, exists
}

// EquipmentNameByEdgeId returns a string with node name from the node id
func (t *TopologyGridStruct) EquipmentNameByEdgeId(id int) string {
	if idx, exists := t.edgeIdxFromEdgeId[id]; exists {
		return t.EquipmentNameByEdgeIdx(idx)
	} else {
		return ""
	}
}

// EquipmentNameByEdgeIdArray returns a string with node names separated by ',' from an array of node ids
func (t *TopologyGridStruct) EquipmentNameByEdgeIdArray(idArray []int) string {
	var name string
	for i, id := range idArray {
		if i != 0 {
			name += ","
		}
		name += t.EquipmentNameByEdgeId(id)
	}
	return name
}

// EquipmentIdByEdgeId returns equipment identifier by corresponded edge id
func (t *TopologyGridStruct) EquipmentIdByEdgeId(edgeId int) (int, error) {
	if edgeIdx, exists := t.edgeIdxFromEdgeId[edgeId]; exists {
		return t.edges[edgeIdx].equipmentId, nil
	}
	return 0, errors.New(fmt.Sprintf("EquipmentIdByEdgeId: edge idx was not found for edge id %d", edgeId))
}

// SetSwitchStateByEquipmentId set switchState field and changes current topology graph
func (t *TopologyGridStruct) SetSwitchStateByEquipmentId(equipmentId int, switchState int) error {
	var err error = nil

	if equipment, exists := t.equipment[equipmentId]; exists {
		equipment.switchState = switchState
		t.equipment[equipmentId] = equipment

		var cost int64
		if equipment.typeId == TypeCircuitBreaker {
			cost = 1
		} else if equipment.typeId == TypeDisconnectSwitch {
			cost = 0
		} else {
			return errors.New(fmt.Sprintf("equipment id %d is not a switch", equipmentId))
		}

		for _, edgeId := range t.edgeIdArrayFromEquipmentId[equipmentId] {
			if edgeIdx, exists := t.edgeIdxFromEdgeId[edgeId]; exists {
				edge := t.edges[edgeIdx]

				node1idx, existsNode1 := t.nodeIdxFromNodeId[edge.terminal.node1Id]
				node2idx, existsNode2 := t.nodeIdxFromNodeId[edge.terminal.node2Id]

				if existsNode1 && existsNode2 {
					if switchState == 1 {
						t.Lock()
						t.currentGraph.AddBothCost(node1idx, node2idx, cost)
						t.Unlock()
					} else {
						t.Lock()
						t.currentGraph.DeleteBoth(node1idx, node2idx)
						t.Unlock()
					}
				} else {
					return errors.New(fmt.Sprintf("Nodes %d:%d are not found", edge.terminal.node1Id, edge.terminal.node2Id))
				}
			}
		}

	} else {
		err = errors.New(fmt.Sprintf("%d - no such equipment", equipmentId))
	}

	return err
}

// AddNode to grid topology
func (t *TopologyGridStruct) AddNode(id int, equipmentId int, equipmentTypeId int, equipmentName string) {

	if equipmentId != 0 {
		t.equipment[equipmentId] = EquipmentStruct{
			id:              equipmentId,
			typeId:          equipmentTypeId,
			name:            equipmentName,
			electricalState: StateIsolated,
			poweredBy:       make(map[int]int64),
		}
	}

	t.nodes[t.nodeIdx] = NodeStruct{idx: t.nodeIdx, id: id, equipmentId: equipmentId}

	t.nodeIdxFromNodeId[id] = t.nodeIdx

	if _, exists := t.nodeIdArrayFromEquipmentId[equipmentId]; !exists {
		t.nodeIdArrayFromEquipmentId[equipmentId] = make([]int, 0)
	}
	t.nodeIdArrayFromEquipmentId[equipmentId] = append(t.nodeIdArrayFromEquipmentId[equipmentId], id)

	if _, exists := t.nodeIdArrayFromEquipmentTypeId[equipmentTypeId]; !exists {
		t.nodeIdArrayFromEquipmentTypeId[equipmentTypeId] = make([]int, 0)
	}
	t.nodeIdArrayFromEquipmentTypeId[equipmentTypeId] = append(t.nodeIdArrayFromEquipmentTypeId[equipmentTypeId], id)

	t.nodeIdx += 1
}

// AddEdge to grid topology
func (t *TopologyGridStruct) AddEdge(id int, terminal1 int, terminal2 int, state int, equipmentId int, equipmentTypeId int, equipmentName string) error {
	terminal := TerminalStruct{node1Id: terminal1, node2Id: terminal2}
	t.edges = append(t.edges,
		EdgeStruct{idx: t.edgeIdx,
			id:          id,
			equipmentId: equipmentId,
			terminal:    terminal,
		})

	if equipmentId != 0 {
		t.equipment[equipmentId] = EquipmentStruct{id: equipmentId,
			typeId:          equipmentTypeId,
			name:            equipmentName,
			electricalState: StateIsolated,
			poweredBy:       make(map[int]int64),
			switchState:     state,
		}
	}

	t.edgeIdxFromEdgeId[id] = t.edgeIdx

	if _, exists := t.nodeIdArrayFromEquipmentId[equipmentId]; !exists {
		t.nodeIdArrayFromEquipmentId[equipmentId] = make([]int, 0)
	}
	t.nodeIdArrayFromEquipmentId[equipmentId] = append(t.nodeIdArrayFromEquipmentId[equipmentId], terminal1)
	t.nodeIdArrayFromEquipmentId[equipmentId] = append(t.nodeIdArrayFromEquipmentId[equipmentId], terminal2)

	if _, exists := t.edgeIdArrayFromEquipmentId[equipmentId]; !exists {
		t.edgeIdArrayFromEquipmentId[equipmentId] = make([]int, 0)
	}
	t.edgeIdArrayFromEquipmentId[equipmentId] = append(t.edgeIdArrayFromEquipmentId[equipmentId], id)

	if _, exists := t.edgeIdArrayFromTerminalStruct[terminal]; !exists {
		t.edgeIdArrayFromTerminalStruct[terminal] = make([]int, 0)
	}

	t.edgeIdArrayFromTerminalStruct[terminal] = append(t.edgeIdArrayFromTerminalStruct[terminal], id)

	if _, exists := t.edgeIdArrayFromEquipmentTypeId[equipmentTypeId]; !exists {
		t.edgeIdArrayFromEquipmentTypeId[equipmentTypeId] = make([]int, 0)
	}

	t.edgeIdArrayFromEquipmentTypeId[equipmentTypeId] = append(t.edgeIdArrayFromEquipmentTypeId[equipmentTypeId], id)

	if _, exists := t.edgeIdArrayFromNodeId[terminal1]; !exists {
		t.edgeIdArrayFromNodeId[terminal1] = make([]int, 0)
	}

	t.edgeIdArrayFromNodeId[terminal1] = append(t.edgeIdArrayFromNodeId[terminal1], id)

	if _, exists := t.edgeIdArrayFromNodeId[terminal2]; !exists {
		t.edgeIdArrayFromNodeId[terminal2] = make([]int, 0)
	}

	t.edgeIdArrayFromNodeId[terminal2] = append(t.edgeIdArrayFromNodeId[terminal2], id)

	t.edgeIdx += 1

	node1idx, existsNode1 := t.nodeIdxFromNodeId[terminal1]
	node2idx, existsNode2 := t.nodeIdxFromNodeId[terminal2]

	// Edge cost == 0 but for Circuit Breaker cost == 1, so we can calculate the shortest path between two nodes
	// to know how many CBs between ones
	var cost int64 = 0
	if equipmentTypeId == TypeCircuitBreaker {
		cost = 1
	}

	if existsNode1 && existsNode2 {
		if state == 1 {
			t.currentGraph.AddBothCost(node1idx, node2idx, cost)
		}

		if equipmentTypeId != TypeDisconnectSwitch || (equipmentTypeId == TypeDisconnectSwitch && state == 1) {
			t.fullGraph.AddBothCost(node1idx, node2idx, cost)
		}

	} else {
		return errors.New(fmt.Sprintf("Nodes %d:%d are not found", terminal1, terminal2))
	}

	return nil
}

// NodeIsPoweredBy returns an array of nodes id with the type of equipment "TypePower"
// from which the specified node is powered with the current switchState of the circuit breakers
func (t *TopologyGridStruct) NodeIsPoweredBy(nodeId int) ([]int, error) {
	poweredBy := make([]int, 0)

	nodeIdx, exists := t.nodeIdxFromNodeId[nodeId]

	if !exists {
		return nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
	}

	for _, nodeTypePowerId := range t.nodeIdArrayFromEquipmentTypeId[TypePower] {

		nodeTypePowerIdx, exists := t.nodeIdxFromNodeId[nodeTypePowerId]

		if !exists {
			return nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
		}

		t.RLock()
		path, _ := graph.ShortestPath(t.currentGraph, nodeTypePowerIdx, nodeIdx)
		t.RUnlock()
		if len(path) > 0 {
			poweredBy = append(poweredBy, nodeTypePowerId)
		}
	}

	return poweredBy, nil
}

// NodeCanBePoweredBy returns an array of nodes id with the type of equipment "Power",
// from which the specified node can be powered regardless of the current switchState of the circuit breakers
func (t *TopologyGridStruct) NodeCanBePoweredBy(nodeId int) ([]int, error) {
	poweredBy := make([]int, 0)

	nodeIdx, exists := t.nodeIdxFromNodeId[nodeId]

	if !exists {
		return nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
	}

	for _, nodeTypePowerId := range t.nodeIdArrayFromEquipmentTypeId[TypePower] {

		nodeTypePowerIdx, exists := t.nodeIdxFromNodeId[nodeTypePowerId]

		if !exists {
			return nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
		}

		t.RLock()
		path, _ := graph.ShortestPath(t.fullGraph, nodeTypePowerIdx, nodeIdx)
		t.RUnlock()

		if len(path) > 0 {
			poweredBy = append(poweredBy, nodeTypePowerId)
		}
	}

	return poweredBy, nil
}

// GetCircuitBreakersEdgeIdsNextToNode returns an array of circuit breakers id next to the node and map with visited equipment ids
func (t *TopologyGridStruct) GetCircuitBreakersEdgeIdsNextToNode(nodeId int) ([]int, map[int]bool, error) {
	var exists bool
	var nodeIdx int
	var edgeCircuitBreakerIdx int
	var visitedNodes = make(map[int]bool)

	circuitBreakersEdgesId := make([]int, 0)

	nodeIdx, exists = t.nodeIdxFromNodeId[nodeId]

	if !exists {
		return nil, nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
	}

	for _, edgeCircuitBreakerId := range t.edgeIdArrayFromEquipmentTypeId[TypeCircuitBreaker] {

		edgeCircuitBreakerIdx, exists = t.edgeIdxFromEdgeId[edgeCircuitBreakerId]

		if !exists {
			return nil, nil, errors.New(fmt.Sprintf("node idx was not found for node id %d", nodeId))
		}

		circuitBreaker := t.edges[edgeCircuitBreakerIdx]

		t.RLock()
		path, pathLen := graph.ShortestPath(t.fullGraph, t.nodeIdxFromNodeId[circuitBreaker.terminal.node1Id], nodeIdx)
		t.RUnlock()

		if len(path) > 0 && pathLen == 0 {
			circuitBreakersEdgesId = append(circuitBreakersEdgesId, edgeCircuitBreakerId)
			for _, _nodeIdx := range path {
				equipmentId := t.nodes[_nodeIdx].equipmentId
				visitedNodes[equipmentId] = true
			}
		} else {
			t.RLock()
			path, pathLen = graph.ShortestPath(t.fullGraph, t.nodeIdxFromNodeId[circuitBreaker.terminal.node2Id], nodeIdx)
			t.RUnlock()

			if len(path) > 0 && pathLen == 0 {
				circuitBreakersEdgesId = append(circuitBreakersEdgesId, edgeCircuitBreakerId)
				for _, _nodeIdx := range path {
					equipmentId := t.nodes[_nodeIdx].equipmentId
					visitedNodes[equipmentId] = true
				}
			}
		}
	}

	return circuitBreakersEdgesId, visitedNodes, nil
}

// BfsFromNodeId traverses current graph in breadth-first order starting at nodeStart
func (t *TopologyGridStruct) BfsFromNodeId(nodeIdStart int) []TerminalStruct {

	var path []TerminalStruct

	graph.BFS(graph.Sort(t.currentGraph), t.nodeIdxFromNodeId[nodeIdStart], func(v, w int, c int64) {
		path = append(path, TerminalStruct{node1Id: t.nodes[v].id, node2Id: t.nodes[w].id, numberOfSwitches: c})
	})
	return path
}

// GetAsGraphMl returns a string with a graph represented by the graph modeling language
func (t *TopologyGridStruct) GetAsGraphMl() string {
	var graphMl string
	var graphics string

	const GraphicsPower = "\n    graphics\n    [\n      type \"star6\"\n      fill \"#FF0000\"\n    ]"
	const GraphicsConsumer = "\n    graphics\n    [\n      type \"triangle\"\n      fill \"#FFCC00\"\n    ]"
	const GraphicsJoin = "\n    graphics\n    [\n      type \"ellipse\"\n      fill \"#808080\"\n      w 5.0\n      h 5.0\n    ]"
	const GraphicsLine = "\n    graphics\n    [\n      type \"rectangle\"\n      fill \"#FF8080\"\n      w 40.0\n      h 10.0\n    ]"

	const GraphicsStateOff = "\n    graphics\n    [\n    style \"dotted\"\n      fill \"#000000\"\n    ]"
	const GraphicsCircuitBreakerOn = "\n    graphics\n    [\n    fill \"#FF0000\"\n    ]"
	const GraphicsCircuitBreakerOff = "\n    graphics\n    [\n    style \"dotted\"\n      fill \"#FF0000\"\n    ]"
	const GraphicsDisconnectSwitchOn = "\n    graphics\n    [\n    fill \"#00FF00\"\n    ]"
	const GraphicsDisconnectSwitchOff = "\n    graphics\n    [\n    style \"dotted\"\n      fill \"#00FF00\"\n    ]"

	for _, node := range t.nodes {

		//if t.equipment[node.equipmentId].typeId == TypeConsumer {
		//	continue
		//}

		if t.equipment[node.equipmentId].typeId == TypePower {
			graphics = GraphicsPower
		} else if t.equipment[node.equipmentId].typeId == TypeConsumer {
			graphics = GraphicsConsumer
		} else if t.equipment[node.equipmentId].typeId == TypeLine {
			graphics = GraphicsLine
		} else {
			graphics = GraphicsJoin
		}
		graphMl += fmt.Sprintf("  node [%s\n    id %d\n    label \"%s\"\n  ]\n",
			graphics, node.id, t.equipment[node.equipmentId].name)
	}

	for _, edge := range t.edges {
		graphics = ""

		//nodeIdx := t.nodeIdxFromNodeId[edge.terminal.node1Id]
		//node := t.nodes[nodeIdx]
		//if t.equipment[node.equipmentId].typeId == TypeConsumer {
		//	continue
		//}
		//
		//nodeIdx = t.nodeIdxFromNodeId[edge.terminal.node2Id]
		//node = t.nodes[nodeIdx]
		//if t.equipment[node.equipmentId].typeId == TypeConsumer {
		//	continue
		//}

		if t.equipment[edge.equipmentId].switchState == 0 {
			graphics = GraphicsStateOff
		}

		if t.equipment[edge.equipmentId].typeId == TypeCircuitBreaker {
			if t.equipment[edge.equipmentId].switchState == 1 {
				graphics = GraphicsCircuitBreakerOn
			} else {
				graphics = GraphicsCircuitBreakerOff
			}
		} else if t.equipment[edge.equipmentId].typeId == TypeDisconnectSwitch {
			if t.equipment[edge.equipmentId].switchState == 1 {
				graphics = GraphicsDisconnectSwitchOn
			} else {
				graphics = GraphicsDisconnectSwitchOff
			}
		}

		graphMl += fmt.Sprintf("  edge [%s\n    source %d\n    target %d\n    label \"%s\"\n  ]\n",
			graphics, edge.terminal.node1Id, edge.terminal.node2Id, t.equipment[edge.equipmentId].name)
	}

	return "graph [\n" + graphMl + "]\n"
}

// SetEquipmentElectricalState for all equipment
// TODO: The electrical state of the switches (edges) in the off state must be calculated by more sophisticated algorithm, since its terminals can have different electrical states.
func (t *TopologyGridStruct) SetEquipmentElectricalState() {
	t.Lock()

	for id, equipment := range t.equipment {
		equipment.electricalState = StateIsolated
		t.equipment[id] = equipment
	}

	for idx, node := range t.nodes {
		node.electricalState = StateIsolated
		t.nodes[idx] = node
	}

	for _, nodeIdOfPowerNode := range t.nodeIdArrayFromEquipmentTypeId[TypePower] {
		cost := make(map[int]int64)

		node := t.nodes[t.nodeIdxFromNodeId[nodeIdOfPowerNode]]
		node.electricalState = StateEnergized
		t.nodes[t.nodeIdxFromNodeId[nodeIdOfPowerNode]] = node

		for _, terminal := range t.BfsFromNodeId(nodeIdOfPowerNode) {
			cost[terminal.node2Id] += terminal.numberOfSwitches + cost[terminal.node1Id]

			node := t.nodes[t.nodeIdxFromNodeId[terminal.node1Id]]
			node.electricalState |= StateEnergized
			t.nodes[t.nodeIdxFromNodeId[terminal.node1Id]] = node
			if node.equipmentId != 0 {
				equipment := t.equipment[node.equipmentId]
				equipment.electricalState |= StateEnergized
				equipment.poweredBy[nodeIdOfPowerNode] = cost[terminal.node1Id]
				t.equipment[node.equipmentId] = equipment
			}

			for _, edgeId := range t.edgeIdArrayFromNodeId[node.id] {
				edge := t.edges[t.edgeIdxFromEdgeId[edgeId]]
				if edge.equipmentId != 0 {
					equipment := t.equipment[edge.equipmentId]
					equipment.electricalState |= StateEnergized
					equipment.poweredBy[nodeIdOfPowerNode] = cost[terminal.node1Id]
					t.equipment[edge.equipmentId] = equipment
				}
			}

			node = t.nodes[t.nodeIdxFromNodeId[terminal.node2Id]]
			node.electricalState |= StateEnergized
			t.nodes[t.nodeIdxFromNodeId[terminal.node2Id]] = node
			if node.equipmentId != 0 {
				equipment := t.equipment[node.equipmentId]
				equipment.electricalState |= StateEnergized
				equipment.poweredBy[nodeIdOfPowerNode] = cost[terminal.node2Id]
				t.equipment[node.equipmentId] = equipment
			}

			for _, edgeId := range t.edgeIdArrayFromNodeId[node.id] {
				edge := t.edges[t.edgeIdxFromEdgeId[edgeId]]
				if edge.equipmentId != 0 {
					equipment := t.equipment[edge.equipmentId]
					equipment.electricalState |= StateEnergized
					equipment.poweredBy[nodeIdOfPowerNode] = cost[terminal.node2Id]
					t.equipment[edge.equipmentId] = equipment
				}
			}
		}
	}
	t.Unlock()
}

func (t *TopologyGridStruct) PrintfEquipments(typeId int) {
	fmt.Printf("-- Equipment begin\n")
	for _, equipment := range t.equipment {
		if typeId == TypeAllEquipment || typeId == equipment.typeId {
			fmt.Printf("%4d:%30s:%2d:%2d <- %+v\n", equipment.id, equipment.name, equipment.switchState, equipment.electricalState, equipment.poweredBy)
		}
	}
	fmt.Printf("-- Equipment end\n")
}

// GetFurthestEquipmentFromPower returns the furthest equipment from the power supply, the ID of the power supply node,
// and the number of switches between the power supply and the equipment
func (t *TopologyGridStruct) GetFurthestEquipmentFromPower(equipmentIds []int) (int, int, int64) {
	var furthestEquipmentId = 0
	var poweredByNodeId = 0

	poweredBy := make(map[int]int64)

	for _, equipmentId := range equipmentIds {
		equipment := t.equipment[equipmentId]
		if equipment.switchState == 0 {
			continue
		}
		for _poweredByNodeId, numberOfSwitches := range equipment.poweredBy {
			if poweredBy[_poweredByNodeId] < numberOfSwitches {
				poweredBy[_poweredByNodeId] = numberOfSwitches
				furthestEquipmentId = equipmentId
				poweredByNodeId = _poweredByNodeId
			}
		}
	}

	return furthestEquipmentId, poweredByNodeId, poweredBy[poweredByNodeId]
}

// GetFurthestEquipmentTerminalIdFromPower returns the farthest (from two) equipment node id (terminal) from the power source
func (t *TopologyGridStruct) GetFurthestEquipmentTerminalIdFromPower(poweredByNodeId int, equipmentId int) int {
	var furthestNodeId = 0
	var maxNumberOfSwitches int64 = 0

	for _, nodeId := range t.nodeIdArrayFromEquipmentId[equipmentId] {
		t.RLock()
		_, numberOfSwitches := graph.ShortestPath(t.currentGraph, t.nodeIdxFromNodeId[nodeId], t.nodeIdxFromNodeId[poweredByNodeId])
		t.RUnlock()
		if maxNumberOfSwitches < numberOfSwitches {
			maxNumberOfSwitches = numberOfSwitches
			furthestNodeId = nodeId
		}
	}

	return furthestNodeId
}

// GetCbListToEnergizeEquipment Returns a map of lists with equipment id of CBs that you must use to power up the selected equipment.
// The mapping keys are the equipment identifier of the power nodes.
func (t *TopologyGridStruct) GetCbListToEnergizeEquipment(equipmentId int) map[int][]int {

	cbListToEnergizeEquipment := make(map[int][]int)

	for _, nodeId := range t.nodeIdArrayFromEquipmentId[equipmentId] {
		if powerNodeIdArray, err := t.NodeCanBePoweredBy(nodeId); err == nil {

			for _, poweredByNodeId := range powerNodeIdArray {

				pathCb := make(map[int]bool)

				t.RLock()
				path, numberOfSwitches := graph.ShortestPath(t.fullGraph, t.nodeIdxFromNodeId[nodeId], t.nodeIdxFromNodeId[poweredByNodeId])
				t.RUnlock()
				// fmt.Printf("%d-%d:%d [%s]\n", nodeId, poweredByNodeId, numberOfSwitches, t.EquipmentNameByNodeIdxArray(path))
				if numberOfSwitches != 0 {
					if len(path) > 1 {
						for i := 0; i < len(path)-1; i++ {
							terminal := TerminalStruct{
								node1Id: t.nodes[path[i]].id,
								node2Id: t.nodes[path[i+1]].id,
							}

							if edgeIdArray, exists := t.edgeIdArrayFromTerminalStruct[terminal]; exists {
								for _, edgeId := range edgeIdArray {
									if equipmentInPathId, err := t.EquipmentIdByEdgeId(edgeId); err == nil {
										if t.equipment[equipmentInPathId].typeId == TypeCircuitBreaker {
											pathCb[equipmentInPathId] = true
										}
									}
								}
							}

							terminal.node1Id, terminal.node2Id = terminal.node2Id, terminal.node1Id

							if edgeIdArray, exists := t.edgeIdArrayFromTerminalStruct[terminal]; exists {
								for _, edgeId := range edgeIdArray {
									if equipmentInPathId, err := t.EquipmentIdByEdgeId(edgeId); err == nil {
										if t.equipment[equipmentInPathId].typeId == TypeCircuitBreaker {
											pathCb[equipmentInPathId] = true
										}
									}
								}
							}
						}
					}
				}
				if len(pathCb) != 0 {
					powerNodeEquipmentId := t.nodes[t.nodeIdxFromNodeId[poweredByNodeId]].equipmentId
					cbListToEnergizeEquipment[powerNodeEquipmentId] = make([]int, len(pathCb))
					i := 0
					for equipmentCbId := range pathCb {
						cbListToEnergizeEquipment[powerNodeEquipmentId][i] = equipmentCbId
						i += 1
					}
				}
			}
		}
	}

	if len(cbListToEnergizeEquipment) == 0 {
		return nil
	}

	return cbListToEnergizeEquipment
}

// CanBeSwitchedOn Checks whether the CB can be closed based on the electrical condition of its terminals
func (t *TopologyGridStruct) CanBeSwitchedOn(cbEquipmentId int) (bool, error) {
	var equipment EquipmentStruct
	var existsEquipment bool

	if equipment, existsEquipment = t.equipment[cbEquipmentId]; existsEquipment {
		if equipment.switchState == SwitchStateClose {
			return false, ErrSwitchIsAlreadyClosed
		}
	} else {
		return false, ErrEquipmentNotFound
	}

	if edgeIdArray, exists := t.edgeIdArrayFromEquipmentId[cbEquipmentId]; exists {
		for _, edgeId := range edgeIdArray {
			edge := t.edges[t.edgeIdxFromEdgeId[edgeId]]

			terminals := edge.terminal

			terminal1Node := t.nodes[t.nodeIdxFromNodeId[terminals.node1Id]]
			terminal2Node := t.nodes[t.nodeIdxFromNodeId[terminals.node2Id]]

			//fmt.Printf("%s %+v %+v\n", equipment.name, terminal1Node, terminal2Node)

			if terminal1Node.electricalState == StateIsolated ||
				terminal2Node.electricalState == StateIsolated {
				return true, nil
			} else if terminal1Node.electricalState&StateGrounded == StateGrounded &&
				terminal2Node.electricalState&StateGrounded == StateGrounded {
				return true, nil
			} else if terminal1Node.electricalState&StateEnergized == StateEnergized &&
				terminal2Node.electricalState&StateEnergized == StateEnergized {
				return false, ErrBothAreEnergized
			} else if terminal1Node.electricalState&StateGrounded == StateGrounded &&
				terminal2Node.electricalState&StateEnergized == StateEnergized {
				return false, ErrEnergizedWillBeGrounded
			} else if terminal1Node.electricalState&StateEnergized == StateEnergized &&
				terminal2Node.electricalState&StateGrounded == StateGrounded {
				return false, ErrEnergizedWillBeGrounded
			}
		}
	}

	return false, ErrEquipmentNotFound
}

// CopyEquipmentSwitchStateFrom form one topogrid object to this
func (t *TopologyGridStruct) CopyEquipmentSwitchStateFrom(source *TopologyGridStruct) error {
	source.RLock()
	for _, equipment := range source.equipment {
		if equipment.typeId == TypeCircuitBreaker {
			if err := t.SetSwitchStateByEquipmentId(equipment.id, equipment.switchState); err != nil {
				source.RUnlock()
				return fmt.Errorf("unable copy switch state for equipment %d:%s", equipment.id, equipment.name)
			}
		}
	}
	source.RUnlock()
	return nil
}
