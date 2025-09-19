package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/internal/mono"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// getClusterByServiceID 通过 service_id 获取对应的 KubeBlocks Cluster，
// 排除已经重备份恢复的 Cluster 替代的 Cluster
// 优先 MatchingFields，失败回退到 MatchingLabels
func getClusterByServiceID(ctx context.Context, c client.Client, serviceID string) (*kbappsv1.Cluster, error) {
	var list kbappsv1.ClusterList

	// 使用 index
	if err := c.List(ctx, &list, client.MatchingFields{index.ServiceIDField: serviceID}); err == nil {
		filteredClusters := filterExcludedClusters(list.Items)
		switch len(filteredClusters) {
		case 0:
			return nil, ErrTargetNotFound
		case 1:
			return &filteredClusters[0], nil
		default:
			return nil, ErrMultipleFounded
		}
	}

	// 回退到 MatchingLabels
	list = kbappsv1.ClusterList{}
	if err := c.List(ctx, &list, client.MatchingLabels{index.ServiceIDLabel: serviceID}); err != nil {
		return nil, fmt.Errorf("list clusters by service_id %s: %w", serviceID, err)
	}

	filteredClusters := filterExcludedClusters(list.Items)
	switch len(filteredClusters) {
	case 0:
		return nil, ErrTargetNotFound
	case 1:
		return &filteredClusters[0], nil
	default:
		return nil, ErrMultipleFounded
	}
}

// getComponentByServiceID 通过 service_id 获取对应的 KubeBlocks Component（Rainbond 侧的 Deployment）
// 优先使用 MatchingFields，失败回退到 MatchingLabels
func getComponentByServiceID(ctx context.Context, c client.Client, serviceID string) (*appsv1.Deployment, error) {
	var list appsv1.DeploymentList

	if err := c.List(ctx, &list, client.MatchingFields{index.ServiceIDField: serviceID}); err == nil {
		switch len(list.Items) {
		case 0:
			return nil, ErrTargetNotFound
		case 1:
			return &list.Items[0], nil
		default:
			return nil, ErrMultipleFounded
		}
	}

	list = appsv1.DeploymentList{}
	if err := c.List(ctx, &list, client.MatchingLabels{index.ServiceIDLabel: serviceID}); err != nil {
		return nil, fmt.Errorf("list deployments by service_id %s: %w", serviceID, err)
	}

	switch len(list.Items) {
	case 0:
		return nil, ErrTargetNotFound
	case 1:
		return &list.Items[0], nil
	default:
		return nil, ErrMultipleFounded
	}
}

// paginate 分页, 从 items 中提取指定页的数据
func paginate[T any](items []T, page, pageSize int) []T {
	if page < 1 || pageSize < 1 || len(items) == 0 {
		return nil
	}

	offset := (page - 1) * pageSize
	if offset >= len(items) {
		return nil
	}

	end := min(offset+pageSize, len(items))
	return items[offset:end:end]
}

// sliceToSet 将字符串切片转换为集合。
func sliceToSet(slice []string) map[string]bool {
	set := make(map[string]bool, len(slice))
	for _, item := range slice {
		set[item] = true
	}
	return set
}

// inferParameterType 从参数值推断参数类型
func inferParameterType(value any) model.ParameterType {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case int, int32, int64, float32, float64:
		return "integer"
	case bool:
		return "boolean"
	case string:
		// 尝试解析为数字
		if strings.Contains(v, ".") {
			if _, err := strconv.ParseFloat(v, 64); err == nil {
				return "number"
			}
		} else {
			if _, err := strconv.ParseInt(v, 10, 64); err == nil {
				return "integer"
			}
		}

		// 尝试解析为布尔值
		if strings.ToUpper(v) == "ON" || strings.ToUpper(v) == "OFF" ||
			strings.ToLower(v) == "true" || strings.ToLower(v) == "false" {
			return "boolean"
		}

		return "string"
	default:
		return "string"
	}
}

// findFailedCondition 查找失败状态的 Condition
func findFailedCondition(conditions []metav1.Condition) *metav1.Condition {
	for _, cond := range conditions {
		if cond.Status == metav1.ConditionFalse {
			return &cond
		}
	}
	return nil
}

func clusterType(cluster *kbappsv1.Cluster) string {
	return cluster.Spec.ClusterDef
}

// formatToISO8601Time 将标准 time.Time 转为 ISO 8601（RFC3339，UTC）字符串
func formatToISO8601Time(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// formatTimeWithOffset 将时间格式化为带数字时区偏移的 RFC3339 格式
// 形如: 2025-09-09T16:51:59+08:00
func formatTimeWithOffset(t time.Time) string {
	localTime := t.In(time.Local)
	return localTime.Format(time.RFC3339)
}

// hasResourceLimits 检查是否设置了 CPU 或 Memory 资源限制
func hasResourceLimits(limits v1.ResourceList) bool {
	if limits == nil {
		return false
	}

	cpu, hasCPU := limits[v1.ResourceCPU]
	memory, hasMemory := limits[v1.ResourceMemory]

	return (hasCPU && !cpu.IsZero()) || (hasMemory && !memory.IsZero())
}

// filterExcludedClusters 排除已经重备份恢复的 Cluster 替代的 Cluster
func filterExcludedClusters(clusters []kbappsv1.Cluster) []kbappsv1.Cluster {
	return mono.Filter(clusters, func(cluster kbappsv1.Cluster) bool {
		if cluster.Annotations == nil {
			return true
		}
		_, exists := cluster.Annotations[SupersededByRestoreAnnotation]
		return !exists
	})
}
