package model

// Addon 表示 KubeBlocks 支持的数据库类型及其版本
type Addon struct {
	Type    string   `json:"type"`
	Version []string `json:"version"`
}

type StorageClasses []string

// KubeBlocksComponentInfo 包含 KubeBlocks Component 的详细信息
type KubeBlocksComponentInfo struct {
	IsKubeBlocksComponent bool   `json:"isKubeBlocksComponent"`
	DatabaseType          string `json:"databaseType,omitempty"`
}
