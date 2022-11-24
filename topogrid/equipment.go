package topogrid

// Equipment electrical states
const (
	StateIsolated    uint8 = 0x00
	StateEnergized   uint8 = 0x01
	StateGrounded    uint8 = 0x02
	StateOvercurrent uint8 = 0x04
	StateFault       uint8 = 0x08
)

// Equipment Types
const (
	TypeAllEquipment     = 0
	TypeCircuitBreaker   = 1
	TypeDisconnectSwitch = 2
	TypePower            = 3
	TypeConsumer         = 4
	TypeGround           = 5
	TypeLine             = 6
)
