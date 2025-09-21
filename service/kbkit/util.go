package kbkit

import (
	"context"
	"fmt"

	"github.com/furutachiKurea/block-mechanica/internal/index"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetClusterByServiceID 通过 service_id 获取对应的 KubeBlocks Cluster
// 优先使用 MatchingFields 索引查询，失败时回退到 MatchingLabels
func GetClusterByServiceID(ctx context.Context, c client.Client, serviceID string) (*kbappsv1.Cluster, error) {
	var list kbappsv1.ClusterList

	// 使用 index
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

	// 回退到 MatchingLabels
	list = kbappsv1.ClusterList{}
	if err := c.List(ctx, &list, client.MatchingLabels{index.ServiceIDLabel: serviceID}); err != nil {
		return nil, fmt.Errorf("list clusters by service_id %s: %w", serviceID, err)
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

// Paginate 分页, 从 items 中提取指定页的数据
func Paginate[T any](items []T, page, pageSize int) []T {
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

func ClusterType(cluster *kbappsv1.Cluster) string {
	return cluster.Spec.ClusterDef
}
