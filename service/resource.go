package service

// resource.go 提供集群资源相关操作
// resource.go 提供集群资源的相关操作

import (
	"context"
	"fmt"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/internal/mono"
	appsv1 "k8s.io/api/apps/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceService 提供集群资源相关操作
type ResourceService struct {
	client client.Client
}

func NewResourceService(c client.Client) *ResourceService {
	return &ResourceService{
		client: c,
	}
}

// GetStorageClasses 返回集群中所有的 StorageClass 的名称
func (s *ResourceService) GetStorageClasses(ctx context.Context) (model.StorageClasses, error) {
	var scList storagev1.StorageClassList
	if err := s.client.List(ctx, &scList); err != nil {
		return nil, fmt.Errorf("list StorageClass: %w", err)
	}
	names := make([]string, 0, len(scList.Items))
	for _, sc := range scList.Items {
		names = append(names, sc.Name)
	}

	return mono.Sorted(names), nil
}

// GetAddons 获取所有可用的 Addon（数据库类型与版本）
func (s *ResourceService) GetAddons(ctx context.Context) ([]*model.Addon, error) {
	var cmpvList kbappsv1.ComponentVersionList
	if err := s.client.List(ctx, &cmpvList); err != nil {
		return nil, fmt.Errorf("get component version list: %w", err)
	}

	addons := make([]*model.Addon, 0, len(cmpvList.Items))
	for _, item := range cmpvList.Items {
		releases := make([]string, 0, len(item.Spec.Releases))
		for _, release := range item.Spec.Releases {
			releases = append(releases, release.ServiceVersion)
		}

		addon := &model.Addon{
			Type:    item.Name,
			Version: mono.Sorted(releases),
		}
		addons = append(addons, addon)
	}

	return mono.FilterThenSort(addons, filterSupportedAddons, func(a, b *model.Addon) bool {
		return a.Type < b.Type
	}), nil
}

// CheckKubeBlocksComponent 依据 RBDService 判定该 Rainbond 组件是否为 KubeBlocks Component，如果是，则还返回 KubeBlocks Component 对应的 Cluster 的数据库类型
//
// 如果给定的 req.RBDService.ID 能够匹配到一个 KubeBlocks Cluster，则说明该 Rainbond 组件为 KubeBlocks Component
func (s *ResourceService) CheckKubeBlocksComponent(ctx context.Context, rbd model.RBDService) (*model.KubeBlocksComponentInfo, error) {
	cluster, err := getClusterByServiceID(ctx, s.client, rbd.ServiceID)
	info := &model.KubeBlocksComponentInfo{IsKubeBlocksComponent: err == nil}
	if err == nil {
		info.DatabaseType = cluster.Spec.ClusterDef
	}

	return info, nil
}

// GetClusterByServiceID 通过 service_id 获取对应的 KubeBlocks Cluster
//
// 封装 GetClusterByServiceID 方法
func (s *ResourceService) GetClusterByServiceID(ctx context.Context, serviceID string) (*kbappsv1.Cluster, error) {
	return getClusterByServiceID(ctx, s.client, serviceID)
}

// GetKubeBlocksComponentByServiceID 通过 service_id 获取对应的 KubeBlocks Component（Rainbond 侧的 Deployment）
//
// 封装 getComponentByServiceID 方法
func (s *ResourceService) GetKubeBlocksComponentByServiceID(ctx context.Context, serviceID string) (*appsv1.Deployment, error) {
	return getComponentByServiceID(ctx, s.client, serviceID)
}

// GetClusterPort 返回指定数据库在 KubeBlocks service 中的目标端口
func (s *ResourceService) GetClusterPort(ctx context.Context, serviceID string) int {
	cluster, err := getClusterByServiceID(ctx, s.client, serviceID)
	if err != nil {
		return -1
	}
	adapter, ok := _clusterRegistry[cluster.Spec.ClusterDef]
	if !ok {
		return -1
	}
	return adapter.Coordinator.TargetPort()
}

// filterSupportedAddons mono.Filter 的过滤函数
//
// 仅返回在 _clusterRegistry 中声明过的数据库类型，确保返回值与系统实际可创建的类型一致。
// 判定是否受 Block Mechanica 支持时, 不同 toplogy 的 addon 视为同一类型
func filterSupportedAddons(addon *model.Addon) bool {
	t := addon.Type
	/*     if i := strings.LastIndex(t, "-"); i > 0 {
	       t = t[:i]
	   } */
	_, ok := _clusterRegistry[t]
	return ok
}

// getClusterByServiceID 通过 service_id 获取对应的 KubeBlocks Cluster
//
// 优先 MatchingFields，失败回退到 MatchingLabels
func getClusterByServiceID(ctx context.Context, c client.Client, serviceID string) (*kbappsv1.Cluster, error) {
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

// getComponentByServiceID 通过 service_id 获取对应的 KubeBlocks Component（Rainbond 侧的 Deployment）
//
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
