package message

type OutputMessageStruct struct {
	Cls    string   `json:"cls"`
	Column []string `json:"c"`
}

func New() *OutputMessageStruct {
	return &OutputMessageStruct{
		Column: make([]string, 2),
	}
}

func (m *OutputMessageStruct) CreateMessage(class string, equipment string, text string) {
	m.Cls = class
	m.Column[0] = equipment
	m.Column[1] = text
}
