// Package builder 提供构建 adapter.ClusterBuilder 的 Builder 实现
//
// ClusterBuilder 用于在 Rainbond 中 KubeBlocks Cluster 的创建
package builder

import (
	"crypto/md5"
	"fmt"
	"time"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/adapter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ adapter.ClusterBuilder = &BaseBuilder{}

// BaseBuilder 实现 ClusterBuilder 接口，所有的 Builder 都应基于 BaseBuilder 实现
type BaseBuilder struct{}

// generateShortName 生成基于哈希的短名称，确保唯一性且长度可控
// 格式：{originalName}-{hash4}，其中 hash4 是 MD5 哈希的前4位十六进制字符
func (b BaseBuilder) generateShortName(originalName string) string {
	timestamp := time.Now().UnixNano()
	input := fmt.Sprintf("%s-%d", originalName, timestamp)

	hash := md5.Sum([]byte(input))

	hashSuffix := fmt.Sprintf("%x", hash[:2])

	return fmt.Sprintf("%s-%s", originalName, hashSuffix)
}

func (b BaseBuilder) BuildCluster(input model.ClusterInput) (*kbappsv1.Cluster, error) {
	cpuQuantity, err := resource.ParseQuantity(input.CPU)
	if err != nil {
		return nil, fmt.Errorf("invalid CPU quantity: %w", err)
	}
	memoryQuantity, err := resource.ParseQuantity(input.Memory)
	if err != nil {
		return nil, fmt.Errorf("invalid memory quantity: %w", err)
	}
	diskQuantity, err := resource.ParseQuantity(input.Storage)
	if err != nil {
		return nil, fmt.Errorf("invalid disk quantity: %w", err)
	}

	// 生成短名称，避免同团队内重名
	clusterName := b.generateShortName(input.Name)

	cluster := &kbappsv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: input.Namespace,
		},
		Spec: kbappsv1.ClusterSpec{
			TerminationPolicy: input.TerminationPolicy,
			ClusterDef:        input.Type,
			ComponentSpecs: []kbappsv1.ClusterComponentSpec{
				{
					Name:           input.Type,
					ServiceVersion: input.Version,
					Replicas:       input.Replicas,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    cpuQuantity,
							corev1.ResourceMemory: memoryQuantity,
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    cpuQuantity,
							corev1.ResourceMemory: memoryQuantity,
						},
					},
					VolumeClaimTemplates: []kbappsv1.ClusterComponentVolumeClaimTemplate{
						{
							Name: "data",
							Spec: kbappsv1.PersistentVolumeClaimSpec{
								StorageClassName: ptr.To(input.StorageClass),
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: diskQuantity,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if input.BackupRepo != "" {
		cluster.Spec.Backup = &kbappsv1.ClusterBackup{
			RepoName:        input.BackupRepo,
			Enabled:         ptr.To(true),
			CronExpression:  input.Schedule.Cron(),
			RetentionPeriod: input.RetentionPeriod,
		}
	}

	return cluster, nil
}
