package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/kbkit"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	workloadsv1 "github.com/apecloud/kubeblocks/apis/workloads/v1"
	"github.com/apecloud/kubeblocks/pkg/constant"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	MiB = 1024 * 1024
	GiB = 1024 * 1024 * 1024
)

// Service 提供针对 Cluster 相关操作
type Service struct {
	client client.Client
}

func NewService(c client.Client) *Service {
	return &Service{
		client: c,
	}
}

// formatToISO8601Time 将标准 time.Time 转为 ISO 8601（RFC3339，UTC）字符串
func formatToISO8601Time(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// AssociateToKubeBlocksComponent 将 KubeBlocks 组件和 Cluster 通过 service_id 关联
func (s *Service) associateToKubeBlocksComponent(ctx context.Context, cluster *kbappsv1.Cluster, serviceID string) error {
	log.Debug("start associate cluster to rainbond component",
		log.String("service_id", serviceID),
		log.String("cluster", cluster.Name),
	)

	const labelServiceID = index.ServiceIDLabel

	err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		var latest kbappsv1.Cluster
		if err := s.client.Get(ctx, client.ObjectKey{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}, &latest); err != nil {
			log.Debug("Cluster not found yet, waiting",
				log.String("cluster", cluster.Name),
				log.String("namespace", cluster.Namespace),
			)
			return false, nil
		}

		if latest.Labels != nil && latest.Labels[labelServiceID] == serviceID {
			log.Debug("Cluster already has correct service_id label",
				log.String("service_id", serviceID),
			)
			return true, nil
		}

		patchData := fmt.Sprintf(`{
			"metadata": {
				"labels": {
					"%s": "%s"
				}
			}
		}`, labelServiceID, serviceID)

		if err := s.client.Patch(ctx, &kbappsv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}, client.RawPatch(types.MergePatchType, []byte(patchData))); err != nil {
			log.Debug("Patch operation failed, retrying",
				log.String("cluster", cluster.Name),
				log.Err(err),
			)
			return false, nil
		}

		log.Debug("Successfully added service_id label to cluster",
			log.String("service_id", serviceID),
			log.String("cluster", cluster.Name),
		)
		return true, nil
	})

	if err != nil {
		return fmt.Errorf("failed to associate cluster %s/%s with service_id label after retries: %w", cluster.Namespace, cluster.Name, err)
	}

	log.Info("Associated KubeBlocks Cluster to Rainbond component",
		log.String("service_id", serviceID),
		log.String("cluster", cluster.Name),
	)

	return nil
}

// getClusterPods 获取 Cluster 相关的 Pod 状态信息
func (s *Service) getClusterPods(ctx context.Context, cluster *kbappsv1.Cluster) ([]model.Status, error) {
	componentName, err := extractComponentName(cluster)
	if err != nil {
		return nil, err
	}

	// 通过 InstanceSet 获取 Pod
	instanceSet, err := getInstanceSetByCluster(ctx, s.client, cluster.Name, cluster.Namespace, componentName)
	if err != nil {
		if errors.Is(err, kbkit.ErrTargetNotFound) {
			// InstanceSet 不存在时返回空列表，而不是错误
			log.Info("InstanceSet not found, returning empty pod list",
				log.String("cluster", cluster.Name),
				log.String("component", componentName))
			return []model.Status{}, nil
		}
		return nil, fmt.Errorf("get instanceset: %w", err)
	}

	var podNames []string
	for _, instanceStatus := range instanceSet.Status.InstanceStatus {
		podNames = append(podNames, instanceStatus.PodName)
	}

	pods, err := getPodsByNames(ctx, s.client, podNames, cluster.Namespace)
	if err != nil {
		return nil, fmt.Errorf("get pods by names: %w", err)
	}

	result := make([]model.Status, 0, len(pods))
	for _, pod := range pods {
		result = append(result, buildPodStatus(pod))
	}

	return result, nil
}

// getPodsByNames 根据 Pod 名称列表查询 Pod
func getPodsByNames(ctx context.Context, c client.Client, podNames []string, namespace string) ([]corev1.Pod, error) {
	var pods []corev1.Pod

	for _, podName := range podNames {
		var pod corev1.Pod
		if err := c.Get(ctx, client.ObjectKey{Name: podName, Namespace: namespace}, &pod); err != nil {
			log.Warn("Failed to get pod", log.String("pod", podName), log.Err(err))
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

// buildPodStatus 构建 Pod 状态信息
func buildPodStatus(pod corev1.Pod) model.Status {
	ready := false

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			ready = true
			break
		}
	}

	return model.Status{
		Name:   pod.Name,
		Status: pod.Status.Phase,
		Ready:  ready,
	}
}

// extractComponentName 从 Cluster 中提取组件名称
func extractComponentName(cluster *kbappsv1.Cluster) (string, error) {
	if len(cluster.Spec.ComponentSpecs) == 0 {
		return "", fmt.Errorf("cluster %s/%s has no componentSpecs", cluster.Namespace, cluster.Name)
	}

	componentName := cluster.Spec.ComponentSpecs[0].Name
	if componentName == "" {
		componentName = cluster.Spec.ClusterDef
	}
	return componentName, nil
}

// getInstanceSetByCluster 通过 cluster 和 component 获取 InstanceSet
func getInstanceSetByCluster(ctx context.Context, c client.Client, clusterName, namespace, componentName string) (*workloadsv1.InstanceSet, error) {
	var instanceSetList workloadsv1.InstanceSetList

	// 优先使用索引查询
	indexKey := fmt.Sprintf("%s/%s/%s", namespace, clusterName, componentName)
	if err := c.List(ctx, &instanceSetList, client.MatchingFields{index.NamespaceClusterComponentField: indexKey}); err == nil {
		switch len(instanceSetList.Items) {
		case 0:
			return nil, kbkit.ErrTargetNotFound
		case 1:
			return &instanceSetList.Items[0], nil
		default:
			return nil, kbkit.ErrMultipleFounded
		}
	} else {
		log.Warn("Index query failed, falling back to label query",
			log.String("indexKey", indexKey), log.Err(err))
	}

	// 回退到标签查询
	selector := client.MatchingLabels{
		constant.AppInstanceLabelKey:        clusterName,
		"apps.kubeblocks.io/component-name": componentName,
	}
	if err := c.List(ctx, &instanceSetList, selector, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list instanceset for cluster %s component %s: %w", clusterName, componentName, err)
	}

	switch len(instanceSetList.Items) {
	case 0:
		return nil, kbkit.ErrTargetNotFound
	case 1:
		return &instanceSetList.Items[0], nil
	default:
		return nil, kbkit.ErrMultipleFounded
	}
}
