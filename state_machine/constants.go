package state_machine

type Condition int

const (
	ConditionIsNotDefine      Condition = 0
	ConditionMeasure          Condition = 1
	ConditionSwitch           Condition = 2
	ConditionControl          Condition = 3
	ConditionProtect          Condition = 4
	ConditionLink             Condition = 5
	ConditionChangeSetGroup   Condition = 6
	ConditionReclosing        Condition = 7
	ConditionStateLineSegment Condition = 8
	ConditionSwitchOn         Condition = 9
	ConditionSwitchOff        Condition = 10
	ConditionTimeout          Condition = 11
)

func ConditionFromName(conditionName string) Condition {
	switch conditionName {
	case "ConditionIsNotDefine":
		return ConditionIsNotDefine
	case "ConditionMeasure":
		return ConditionMeasure
	case "ConditionSwitch":
		return ConditionSwitch
	case "ConditionControl":
		return ConditionControl
	case "ConditionProtect":
		return ConditionProtect
	case "ConditionLink":
		return ConditionLink
	case "ConditionChangeSetGroup":
		return ConditionChangeSetGroup
	case "ConditionReclosing":
		return ConditionReclosing
	case "ConditionStateLineSegment":
		return ConditionStateLineSegment
	case "ConditionSwitchOn":
		return ConditionSwitchOn
	case "ConditionSwitchOff":
		return ConditionSwitchOff
	case "ConditionTimeout":
		return ConditionTimeout
	default:
		return Condition(0)
	}
}

func (condition Condition) Name() string {
	switch condition {
	case ConditionIsNotDefine:
		return "--"
	case ConditionMeasure:
		return "MS"
	case ConditionSwitch:
		return "SW"
	case ConditionSwitchOn:
		return "On"
	case ConditionSwitchOff:
		return "Off"
	case ConditionControl:
		return "CO"
	case ConditionProtect:
		return "PR"
	case ConditionLink:
		return "LK"
	case ConditionChangeSetGroup:
		return "SG"
	case ConditionReclosing:
		return "RC"
	case ConditionStateLineSegment:
		return "LS"
	case ConditionTimeout:
		return "T"
	default:
		return "--"
	}
}
