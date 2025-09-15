// Package coordinator 提供 adapter.Coordinator 的实现
//
// Coordinator 用于协调 KubeBlocks 和 Rainbond
package coordinator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/adapter"
)

var _ adapter.Coordinator = &Base{}

// Base 实现 Coordinator 接口，所有的 Coordinator 都应基于 Base 实现
type Base struct {
}

func (c *Base) TargetPort() int {
	return -1
}

func (c *Base) GetSecretName(clusterName string) string {
	// Base 实现使用通用的 root 账户格式，但实际不应被直接使用
	// 每个具体的 Coordinator 都应该重写此方法
	return fmt.Sprintf("%s-account-root", clusterName)
}

func (c *Base) GetBackupMethod() string {
	// Base 实现返回默认备份方法，但实际不应被直接使用
	// 每个具体的 Coordinator 都应该重写此方法
	return "default"
}

func (c *Base) GetParametersConfigMap(clusterName string) *string {
	return nil
}

func (c *Base) ParseParameters(configData map[string]string) ([]model.ParameterEntry, error) {
	// Base 实现不解析任何配置，返回空结果
	// 每个具体的 Coordinator 都应该重写此方法
	return []model.ParameterEntry{}, nil
}

// convParameterValue 解析配置参数值，尝试转换为合适的类型
// 支持自动类型推断: int -> int, float -> float64, bool -> bool（仅 true/false）,
func convParameterValue(value string) any {
	if value == "" {
		return value
	}

	trimmed := strings.Trim(value, "'\"")

	// bool
	if strings.EqualFold(trimmed, "true") {
		return true
	}
	if strings.EqualFold(trimmed, "false") {
		return false
	}

	// int64
	if intVal, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return intVal
	}

	// uint64
	if uintVal, err := strconv.ParseUint(trimmed, 10, 64); err == nil {
		return uintVal
	}

	// float64
	if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return floatVal
	}

	// 处理带单位的值，应当原样返回
	if len(trimmed) > 1 {
		lastChar := strings.ToUpper(string(trimmed[len(trimmed)-1:]))
		if lastChar == "K" || lastChar == "M" || lastChar == "G" || lastChar == "T" {
			numPart := trimmed[:len(trimmed)-1]
			if _, err := strconv.ParseFloat(numPart, 64); err == nil {
				return trimmed
			}
		}
	}

	// 处理时间单位，应当原样返回
	if len(trimmed) > 1 {
		lastTwo := strings.ToLower(trimmed[len(trimmed)-2:])
		lastOne := strings.ToLower(string(trimmed[len(trimmed)-1:]))
		if lastTwo == "ms" || lastTwo == "us" || lastOne == "s" || lastOne == "m" || lastOne == "h" {
			return trimmed
		}
	}

	return trimmed
}
