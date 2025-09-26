package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/kbkit"
	"github.com/furutachiKurea/block-mechanica/service/registry"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	opsv1alpha1 "github.com/apecloud/kubeblocks/apis/operations/v1alpha1"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateCluster 依据 req 创建 KubeBlocks Cluster
//
// 通过将 service_id 添加至 Cluster 的 labels 中以关联 KubeBlocks Component 与 Cluster,
// 同时，Rainbond 也通过这层关系来判断 Rainbond 组件是否为 KubeBlocks Component
//
// 返回成功创建的 KubeBlocks Cluster 实例
func (s *Service) CreateCluster(ctx context.Context, input model.ClusterInput) (*kbappsv1.Cluster, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	clusterAdapter, ok := registry.Cluster[input.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported cluster type: %s", input.Type)
	}

	cluster, err := clusterAdapter.Builder.BuildCluster(input)
	if err != nil {
		return nil, fmt.Errorf("build %s cluster: %w", input.Type, err)
	}

	if err := s.client.Create(ctx, cluster); err != nil {
		return nil, fmt.Errorf("create cluster: %w", err)
	}

	input.Name = cluster.Name
	if err := s.associateToKubeBlocksComponent(ctx, cluster, input.RBDService.ServiceID); err != nil {
		return nil, fmt.Errorf("associate to rainbond component: %w", err)
	}

	return cluster, nil
}

// DeleteClusters 删除 KubeBlocks 数据库集群
//
// 批量删除指定 serviceIDs 对应的 Cluster，忽略找不到的 service_id
func (s *Service) DeleteClusters(ctx context.Context, serviceIDs []string) error {
	for _, serviceID := range serviceIDs {
		err := s.deleteCluster(ctx, serviceID, false)
		if err != nil {
			if errors.Is(err, kbkit.ErrTargetNotFound) {
				continue
			}
			return fmt.Errorf("delete cluster for service_id %s: %w", serviceID, err)
		}
	}
	return nil
}

// CancelClusterCreate 取消集群创建
//
// 在删除前将 TerminationPolicy 调整为 WipeOut，确保 PVC/PV 等存储资源一并清理，避免脏数据残留
func (s *Service) CancelClusterCreate(ctx context.Context, rbd model.RBDService) error {
	return s.deleteCluster(ctx, rbd.ServiceID, true)
}

// deleteCluster 内部删除方法，提供是否将 TerminationPolicy 设置为 WipeOut 的选项
func (s *Service) deleteCluster(ctx context.Context, serviceID string, isCancle bool) error {
	cluster, err := kbkit.GetClusterByServiceID(ctx, s.client, serviceID)
	if err != nil {
		return fmt.Errorf("get cluster by service_id %s: %w", serviceID, err)
	}

	log.Info("Found cluster for deletion",
		log.String("service_id", serviceID),
		log.String("cluster_name", cluster.Name),
		log.String("namespace", cluster.Namespace),
		log.String("current_termination_policy", string(cluster.Spec.TerminationPolicy)),
		log.Bool("wipe_out", isCancle))

	// 清理 Cluster 的 OpsRequest
	if err := s.cleanupClusterOpsRequests(ctx, cluster); err != nil {
		log.Warn("Failed to cleanup OpsRequests, proceeding with cluster deletion",
			log.String("service_id", serviceID),
			log.String("cluster_name", cluster.Name),
			log.Err(err))
	}

	if isCancle && cluster.Spec.TerminationPolicy != kbappsv1.WipeOut {
		log.Info("Updating TerminationPolicy to WipeOut before deletion",
			log.String("cluster_name", cluster.Name),
			log.String("namespace", cluster.Namespace))

		patch := client.MergeFrom(cluster.DeepCopy())
		cluster.Spec.TerminationPolicy = kbappsv1.WipeOut

		if err := s.client.Patch(ctx, cluster, patch); err != nil {
			return fmt.Errorf("patch cluster %s/%s terminationPolicy to WipeOut: %w",
				cluster.Namespace, cluster.Name, err)
		}

		log.Info("Successfully updated TerminationPolicy to WipeOut",
			log.String("cluster_name", cluster.Name),
			log.String("namespace", cluster.Namespace))
	}

	policy := metav1.DeletePropagationForeground
	deleteOptions := &client.DeleteOptions{
		PropagationPolicy: &policy,
	}

	if err := s.client.Delete(ctx, cluster, deleteOptions); err != nil {
		return fmt.Errorf("delete cluster %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}

	log.Info("Successfully initiated cluster deletion",
		log.String("service_id", serviceID),
		log.String("cluster_name", cluster.Name),
		log.String("namespace", cluster.Namespace),
		log.Bool("wipe_out", isCancle))

	return nil
}

// ManageClustersLifecycle 通过创建 OpsRequest 批量管理多个 Cluster 的生命周期
func (s *Service) ManageClustersLifecycle(ctx context.Context, operation opsv1alpha1.OpsType, serviceIDs []string) *model.BatchOperationResult {
	manageResult := model.NewBatchOperationResult()
	for _, serviceID := range serviceIDs {
		cluster, err := kbkit.GetClusterByServiceID(ctx, s.client, serviceID)
		if errors.Is(err, kbkit.ErrTargetNotFound) {
			continue
		}
		if err != nil {
			manageResult.AddFailed(serviceID, err)
			continue
		}

		if err = kbkit.CreateLifecycleOpsRequest(ctx, s.client, cluster, operation); err == nil {
			manageResult.AddSucceeded(serviceID)
		} else {
			manageResult.AddFailed(serviceID, err)
		}
	}
	return manageResult
}

// cleanupClusterOpsRequests 清理指定 Cluster 的所有 OpsRequest
func (s *Service) cleanupClusterOpsRequests(ctx context.Context, cluster *kbappsv1.Cluster) error {
	// 获取并清理所有非终态 OpsRequest，使其进入终态
	blockingOps, err := kbkit.GetAllNonFinalOpsRequests(ctx, s.client, cluster.Namespace, cluster.Name)
	if err != nil {
		return fmt.Errorf("get existing opsrequests: %w", err)
	}

	if len(blockingOps) > 0 {
		log.Debug("Found blocking OpsRequests, initiating cleanup",
			log.String("cluster", cluster.Name),
			log.Int("blocking_count", len(blockingOps)))

		if err := kbkit.CleanupBlockingOps(ctx, s.client, blockingOps); err != nil {
			return fmt.Errorf("cleanup blocking ops: %w", err)
		}
	}

	// 获取并删除所有 OpsRequest
	allOps, err := kbkit.GetAllOpsRequestsByCluster(ctx, s.client, cluster.Namespace, cluster.Name)
	if err != nil {
		return fmt.Errorf("get all opsrequests: %w", err)
	}

	if len(allOps) == 0 {
		log.Debug("No OpsRequests found for cluster",
			log.String("cluster", cluster.Name))
		return nil
	}

	log.Info("Deleting all OpsRequests for complete cleanup",
		log.String("cluster", cluster.Name),
		log.Int("total_count", len(allOps)))

	// 并发删除所有 OpsRequest，避免孤儿资源
	if err := s.deleteAllOpsRequestsConcurrently(ctx, allOps); err != nil {
		return fmt.Errorf("delete all ops: %w", err)
	}

	log.Info("Successfully cleaned up all OpsRequests",
		log.String("cluster", cluster.Name),
		log.Int("deleted_count", len(allOps)))

	return nil
}

// deleteAllOpsRequestsConcurrently 并发删除所有 OpsRequest
func (s *Service) deleteAllOpsRequestsConcurrently(ctx context.Context, allOps []opsv1alpha1.OpsRequest) error {
	if len(allOps) == 0 {
		return nil
	}

	group, gctx := errgroup.WithContext(ctx)
	for i := range allOps {
		op := &allOps[i]
		group.Go(func() error {
			if err := s.client.Delete(gctx, op); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("failed to delete opsrequest %s: %w", op.Name, err)
			}
			return nil
		})
	}

	return group.Wait()
}
