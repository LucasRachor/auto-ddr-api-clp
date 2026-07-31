package common

import (
	"encoding/binary"
	"fmt"
)

func parseDINTArray(data []byte) ([]int32, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("payload inválido")
	}

	payload := data[2:] // remove o type code

	if len(payload)%4 != 0 {
		return nil, fmt.Errorf("payload não múltiplo de 4")
	}

	result := make([]int32, len(payload)/4)

	for i := 0; i < len(result); i++ {
		offset := i * 4
		result[i] = int32(binary.LittleEndian.Uint32(payload[offset : offset+4]))
	}

	return result, nil
}
