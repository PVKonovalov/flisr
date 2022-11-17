package types

// Resource Types
const (
	ResourceTypeIsNotDefine      = 0
	ResourceTypeMeasure          = 1
	ResourceTypeState            = 2
	ResourceTypeControl          = 3
	ResourceTypeProtect          = 4
	ResourceTypeLink             = 5
	ResourceTypeChangeSetGroup   = 6
	ResourceTypeReclosing        = 7
	ResourceTypeStateLineSegment = 8
)

func ResourceTypeFromString(resourceTypeName string) int {
	switch resourceTypeName {
	case "ResourceTypeIsNotDefine":
		return 0
	case "ResourceTypeMeasure":
		return 1
	case "ResourceTypeState":
		return 2
	case "ResourceTypeControl":
		return 3
	case "ResourceTypeProtect":
		return 4
	case "ResourceTypeLink":
		return 5
	case "ResourceTypeChangeSetGroup":
		return 6
	case "ResourceTypeReclosing":
		return 7
	case "ResourceTypeStateLineSegment":
		return 8
	default:
		return 0
	}
}

func ConditionAlias(condition int) string {
	switch condition {
	case ResourceTypeIsNotDefine:
		return "ND"
	case ResourceTypeMeasure:
		return "MS"
	case ResourceTypeState:
		return "ST"
	case ResourceTypeControl:
		return "CO"
	case ResourceTypeProtect:
		return "PR"
	case ResourceTypeLink:
		return "LK"
	case ResourceTypeChangeSetGroup:
		return "SG"
	case ResourceTypeReclosing:
		return "RC"
	case ResourceTypeStateLineSegment:
		return "LS"
	default:
		return "ND"
	}
}
