package coordinator

import (
	"fmt"
	"strings"

	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/adapter"

	"github.com/spf13/viper"
)

var _ adapter.Coordinator = &MySQL{}

// MySQL 实现 Coordinator 接口
type MySQL struct {
	Base
}

func (c *MySQL) TargetPort() int {
	return 3306
}

func (c *MySQL) GetSecretName(clusterName string) string {
	// MySQL 使用 mysql 作为中间部分和 root 作为账户类型
	return fmt.Sprintf("%s-mysql-account-root", clusterName)
}

func (c *MySQL) GetBackupMethod() string {
	return "xtrabackup"
}

func (c *MySQL) GetParametersConfigMap(clusterName string) *string {
	cmName := fmt.Sprintf("%s-mysql-mysql-replication-config", clusterName)
	return &cmName
}

// ParseParameters 解析 MySQL ConfigMap 中的 my.cnf 配置参数
// 基于实际的 ConfigMap 格式: data.my.cnf 包含 INI 格式的配置内容
func (c *MySQL) ParseParameters(configData map[string]string) ([]model.ParameterEntry, error) {
	// 获取 my.cnf 配置内容
	myCnfContent, exists := configData["my.cnf"]
	if !exists {
		log.Warn("my.cnf not found in ConfigMap data")
		return []model.ParameterEntry{}, nil
	}

	if strings.TrimSpace(myCnfContent) == "" {
		log.Info("my.cnf content is empty")
		return []model.ParameterEntry{}, nil
	}

	v := viper.New()
	v.SetConfigType("ini")

	if err := v.ReadConfig(strings.NewReader(myCnfContent)); err != nil {
		log.Warn("failed to parse my.cnf content", log.Err(err))
		return []model.ParameterEntry{}, fmt.Errorf("parse my.cnf: %w", err)
	}

	var parameters []model.ParameterEntry

	mysqldSettings := v.GetStringMapString("mysqld")
	for key, value := range mysqldSettings {
		param := model.ParameterEntry{
			Name:  key,
			Value: convParameterValue(value),
		}
		parameters = append(parameters, param)
	}

	return parameters, nil
}
