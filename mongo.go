package mongo

import (
	"encoding/binary"
)

var wire = binary.LittleEndian

type DateTime int64

type CodeWithScope struct {
	Code  string
	Scope map[string]interface{}
}

type Regexp struct {
	Pattern string
	Flags   string
}

type ObjectId [12]byte


