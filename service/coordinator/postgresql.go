package coordinator

import (
	"fmt"
	"strings"

	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/adapter"

	"github.com/spf13/viper"
)

var _ adapter.Coordinator = &PostgreSQL{}

// PostgreSQL 实现 Coordinator 接口
type PostgreSQL struct {
	Base
}

func (c *PostgreSQL) TargetPort() int {
	return 6432
}

func (c *PostgreSQL) GetSecretName(clusterName string) string {
	// PostgreSQL 使用 postgresql 作为中间部分和 postgres 作为账户类型
	return fmt.Sprintf("%s-postgresql-account-postgres", clusterName)
}

func (c *PostgreSQL) GetBackupMethod() string {
	return "pg-basebackup"
}

func (c *PostgreSQL) GetParametersConfigMap(clusterName string) *string {
	cmName := fmt.Sprintf("%s-postgresql-postgresql-configuration", clusterName)
	return &cmName
}

// ParseParameters 解析 PostgreSQL ConfigMap 中的 postgresql.conf 配置参数
// 基于实际的 ConfigMap 格式: data.postgresql.conf 包含键值对格式的配置内容
func (c *PostgreSQL) ParseParameters(configData map[string]string) ([]model.ParameterEntry, error) {
	// 获取 postgresql.conf 配置内容
	pgConfContent, exists := configData["postgresql.conf"]
	if !exists {
		log.Warn("postgresql.conf not found in ConfigMap data")
		return []model.ParameterEntry{}, nil
	}

	if strings.TrimSpace(pgConfContent) == "" {
		log.Info("postgresql.conf content is empty")
		return []model.ParameterEntry{}, nil
	}

	v := viper.New()
	v.SetConfigType("properties")

	if err := v.ReadConfig(strings.NewReader(pgConfContent)); err != nil {
		log.Warn("failed to parse postgresql.conf content", log.Err(err))
		return []model.ParameterEntry{}, fmt.Errorf("parse postgresql.conf: %w", err)
	}

	var parameters []model.ParameterEntry

	// 获取所有配置项
	allSettings := v.AllSettings()
	for key, value := range allSettings {
		if strings.HasPrefix(key, "#") {
			continue
		}

		param := model.ParameterEntry{
			Name:  key,
			Value: convParameterValue(fmt.Sprintf("%v", value)), // TODO 替换 Sprintf
		}
		parameters = append(parameters, param)
	}

	return parameters, nil
}
