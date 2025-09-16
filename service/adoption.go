// Package service 提供备份资源继承相关功能
//
// 实现从旧 cluster 到新 cluster 的 backup 资源继承
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/furutachiKurea/block-mechanica/internal/log"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	datav1alpha1 "github.com/apecloud/kubeblocks/apis/dataprotection/v1alpha1"
	"github.com/apecloud/kubeblocks/pkg/constant"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BackupAdoptionState 跟踪备份资源继承操作的状态
type BackupAdoptionState struct {
	BackupsUpdated []string
	StartTime      time.Time
	Completed      bool
	mu             sync.RWMutex
}

// AddBackup 添加已更新的 backup
func (s *BackupAdoptionState) AddBackup(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.BackupsUpdated = append(s.BackupsUpdated, name)
}

// GetState 获取当前状态
func (s *BackupAdoptionState) GetState() ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backups := make([]string, len(s.BackupsUpdated))
	copy(backups, s.BackupsUpdated)

	return backups, s.Completed
}

// MarkAsCompleted 标记操作完成
func (s *BackupAdoptionState) MarkAsCompleted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Completed = true
}

// AdoptionMachine -
type AdoptionMachine struct {
	client         client.Client
	maxBatchSize   int
	timeout        time.Duration
	maxConcurrency int
}

// NewAdoptionMachine -
func NewAdoptionMachine(client client.Client) *AdoptionMachine {
	return &AdoptionMachine{
		client:         client,
		maxBatchSize:   50,
		timeout:        30 * time.Second,
		maxConcurrency: 10,
	}
}

// WithBatchSize 设置批次大小
func (e *AdoptionMachine) WithBatchSize(size int) *AdoptionMachine {
	e.maxBatchSize = size
	return e
}

// WithTimeout 设置操作超时时间
func (e *AdoptionMachine) WithTimeout(timeout time.Duration) *AdoptionMachine {
	e.timeout = timeout
	return e
}

// WithMaxConcurrency 设置最大并发数
func (e *AdoptionMachine) WithMaxConcurrency(maxConcurrency int) *AdoptionMachine {
	e.maxConcurrency = maxConcurrency
	return e
}

// AdoptResources 实现资源继承的核心逻辑
func (e *AdoptionMachine) AdoptResources(ctx context.Context, fromCluster, toCluster *kbappsv1.Cluster) error {
	start := time.Now()
	state := &BackupAdoptionState{
		StartTime: start,
	}

	log.Debug("starting backup resources adoption",
		log.String("from_cluster", fromCluster.Name),
		log.String("to_cluster", toCluster.Name),
		log.String("namespace", fromCluster.Namespace),
	)

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	// 继承 backup
	if err := e.adoptBackupsConcurrently(ctx, fromCluster.Name, toCluster.Name, fromCluster.Namespace, state); err != nil {
		return fmt.Errorf("adopt backups: %w", err)
	}

	state.MarkAsCompleted()

	backups, _ := state.GetState()
	duration := time.Since(start)

	log.Debug("backup resources adoption completed",
		log.String("from_cluster", fromCluster.Name),
		log.String("to_cluster", toCluster.Name),
		log.Int("backups_inherited", len(backups)),
		log.Duration("duration", duration),
	)

	return nil
}

// adoptBackupsConcurrently 并发继承 Backup 资源
func (e *AdoptionMachine) adoptBackupsConcurrently(
	ctx context.Context,
	fromClusterName, toClusterName, namespace string,
	state *BackupAdoptionState,
) error {
	log.Debug("starting backup inheritance",
		log.String("from_cluster", fromClusterName),
		log.String("to_cluster", toClusterName),
	)

	backups, err := getBackupsByIndex(ctx, e.client, fromClusterName, namespace)
	if err != nil {
		return fmt.Errorf("list backups for cluster %s: %w", fromClusterName, err)
	}

	if len(backups) == 0 {
		log.Debug("no backups found for adoption",
			log.String("from_cluster", fromClusterName),
		)
		return nil
	}

	log.Debug("found backups for adoption",
		log.String("from_cluster", fromClusterName),
		log.Int("backup_count", len(backups)),
	)

	// Level 2 并发: 批量并发更新 backup 标签
	return e.updateBackupLabelsConcurrently(ctx, backups, toClusterName, state.AddBackup)
}

// updateBackupLabelsConcurrently 并发更新 backup 标签
func (e *AdoptionMachine) updateBackupLabelsConcurrently(
	ctx context.Context,
	resources []datav1alpha1.Backup,
	newInstanceName string,
	addToState func(string),
) error {
	if len(resources) == 0 {
		return nil
	}

	// 计算批次数量
	batchCount := (len(resources) + e.maxBatchSize - 1) / e.maxBatchSize
	if batchCount > e.maxConcurrency {
		batchCount = e.maxConcurrency
	}

	// 使用 errgroup 管理批量并发操作
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(batchCount) // 限制并发数

	// 分批并发处理
	for i := 0; i < len(resources); i += e.maxBatchSize {
		start := i
		end := min(i+e.maxBatchSize, len(resources))

		batch := resources[start:end]
		g.Go(func() error {
			return e.updateBatchBackupLabels(ctx, batch, newInstanceName, addToState)
		})
	}

	return g.Wait()
}

// updateBatchBackupLabels 批量更新 backup 资源标签
func (e *AdoptionMachine) updateBatchBackupLabels(
	ctx context.Context,
	resources []datav1alpha1.Backup,
	newInstanceName string,
	addToState func(string),
) error {
	for _, resource := range resources {
		// 检查当前标签是否需要更新 backup 的 app.kubernetes.io/instance label
		labels := resource.GetLabels()
		if labels == nil {
			continue
		}

		currentInstance, exists := labels[constant.AppInstanceLabelKey]
		if !exists || currentInstance == newInstanceName {
			continue
		}

		// 构造补丁数据
		patchData := fmt.Sprintf(`{
			"metadata": {
				"labels": {
					"%s": "%s"
				}
			}
		}`, constant.AppInstanceLabelKey, newInstanceName)

		// 执行 label 更新
		if err := e.client.Patch(ctx, &resource, client.RawPatch(types.MergePatchType, []byte(patchData))); err != nil {
			log.Error("failed to update backup label",
				log.String("backup_name", resource.GetName()),
				log.String("namespace", resource.GetNamespace()),
				log.Err(err),
			)
			continue
		}

		addToState(resource.GetName())

		log.Debug("updated backup label",
			log.String("backup_name", resource.GetName()),
			log.String("old_instance", currentInstance),
			log.String("new_instance", newInstanceName),
		)
	}

	return nil
}

// RollbackAdoption 实现备份资源继承操作的回滚
func (e *AdoptionMachine) RollbackAdoption(ctx context.Context, fromCluster, toCluster *kbappsv1.Cluster) error {
	start := time.Now()

	log.Info("starting backup adoption rollback",
		log.String("from_cluster", fromCluster.Name),
		log.String("to_cluster", toCluster.Name),
		log.String("namespace", fromCluster.Namespace),
	)

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	// 回滚 Backup 资源
	if err := e.rollbackBackupsToOriginalCluster(ctx, fromCluster.Name, toCluster.Name, fromCluster.Namespace); err != nil {
		return fmt.Errorf("rollback backups failed: %w", err)
	}

	duration := time.Since(start)
	log.Info("backup inheritance rollback completed",
		log.String("from_cluster", fromCluster.Name),
		log.String("to_cluster", toCluster.Name),
		log.Duration("duration", duration),
	)

	return nil
}

// rollbackBackupsToOriginalCluster 回滚 Backup 资源的标签
func (e *AdoptionMachine) rollbackBackupsToOriginalCluster(
	ctx context.Context,
	originalClusterName, currentClusterName, namespace string,
) error {
	// 查找当前指向新集群的 Backup 资源
	backups, err := getBackupsByIndex(ctx, e.client, currentClusterName, namespace)
	if err != nil {
		return fmt.Errorf("list backups for rollback: %w", err)
	}

	if len(backups) == 0 {
		return nil // 没有需要回滚的资源
	}

	// 并发回滚标签
	return e.updateBackupLabelsConcurrently(ctx, backups, originalClusterName, func(name string) {
		log.Debug("rolled back backup label", log.String("backup_name", name))
	})
}
