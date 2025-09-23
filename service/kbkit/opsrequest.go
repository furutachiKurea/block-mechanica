package kbkit

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"time"

	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/model"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	opsv1alpha1 "github.com/apecloud/kubeblocks/apis/operations/v1alpha1"
	"github.com/apecloud/kubeblocks/pkg/constant"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OpsRequest 配置项
var (
	opsTimeoutSecond      int32 = 24 * 60 * 60
	opsLifeAfterUnsuccess int32 = 1 * 60 * 60
	opsLifeAfterSucceed   int32 = 24 * 60 * 60

	defaultBackupDeletionPolicy = "Delete"
)

const (
	preflightProceed preflightDecision = iota + 1 // 创建
	preflightSkip                                 // 跳过
)

type preflightDecision int

type preflightResult struct {
	Decision preflightDecision
}

// preflight 规定 OpsRequest 创建前/后的决策逻辑
type preflight interface {
	// decide 根据创建目标判断是否允许创建
	decide(ctx context.Context, c client.Client, ops *opsv1alpha1.OpsRequest) (preflightResult, error)
}

// uniqueOps 检查是否存在处在非终态的同类型同目标的 OpsRequest，
type uniqueOps struct{}

func (uniqueOps) decide(ctx context.Context, c client.Client, ops *opsv1alpha1.OpsRequest) (preflightResult, error) {
	opsList, err := getOpsRequestsByIndex(ctx, c, ops.Namespace, ops.Spec.ClusterName, ops.Spec.Type)
	if err != nil {
		if !errors.IsNotFound(err) {
			return preflightResult{}, fmt.Errorf("list opsrequests for preflight: %w", err)
		}
		return preflightResult{Decision: preflightProceed}, nil
	}

	for _, ops := range opsList {
		if !isOpsRequestInFinalPhase(&ops) {
			return preflightResult{Decision: preflightSkip}, nil
		}
	}

	return preflightResult{Decision: preflightProceed}, nil
}

type createOpts struct {
	preflight preflight
}

type createOption func(*createOpts)

// withPreflight 自定义预检策略
func withPreflight(p preflight) createOption {
	return func(o *createOpts) { o.preflight = p }
}

// CreateLifecycleOpsRequest 创建生命周期管理相关的 OpsRequest，供 Reconciler 使用
func CreateLifecycleOpsRequest(ctx context.Context,
	c client.Client,
	cluster *kbappsv1.Cluster,
	opsType opsv1alpha1.OpsType,
) error {
	opsSpecific := opsv1alpha1.SpecificOpsRequest{}
	if opsType == opsv1alpha1.RestartType {
		opsSpecific.RestartList = []opsv1alpha1.ComponentOps{
			{
				ComponentName: ClusterType(cluster),
			},
		}
	}

	if _, err := createOpsRequest(ctx, c, cluster, opsType, opsSpecific, withPreflight(uniqueOps{})); err != nil {
		return err
	}

	return nil
}

// CreateBackupOpsRequest 为指定的 Cluster 创建备份 OpsRequest
//
// backupMethod 为备份方法，取决于数据库类型
func CreateBackupOpsRequest(ctx context.Context,
	c client.Client,
	cluster *kbappsv1.Cluster,
	backupMethod string,
) error {

	specificOps := opsv1alpha1.SpecificOpsRequest{
		Backup: &opsv1alpha1.Backup{
			BackupPolicyName: fmt.Sprintf("%s-%s-backup-policy", cluster.Name, ClusterType(cluster)),
			BackupMethod:     backupMethod,
			DeletionPolicy:   defaultBackupDeletionPolicy,
		},
	}

	_, err := createOpsRequest(ctx, c, cluster, opsv1alpha1.BackupType, specificOps)
	return err
}

// CreateHorizontalScalingOpsRequest 为指定的 Cluster 创建水平伸缩 OpsRequest
func CreateHorizontalScalingOpsRequest(ctx context.Context,
	c client.Client,
	params model.HorizontalScalingOpsParams,
) error {
	var horizontalScalingList []opsv1alpha1.HorizontalScaling

	// 遍历所有组件，为每个组件创建对应的伸缩配置
	for _, component := range params.Components {
		var scaling opsv1alpha1.HorizontalScaling

		if component.DeltaReplicas > 0 {
			// ScaleOut
			scaling = opsv1alpha1.HorizontalScaling{
				ComponentOps: opsv1alpha1.ComponentOps{ComponentName: component.Name},
				ScaleOut: &opsv1alpha1.ScaleOut{
					ReplicaChanger: opsv1alpha1.ReplicaChanger{ReplicaChanges: &component.DeltaReplicas},
				},
			}
		} else {
			// ScaleIn
			absReplicas := -component.DeltaReplicas
			scaling = opsv1alpha1.HorizontalScaling{
				ComponentOps: opsv1alpha1.ComponentOps{ComponentName: component.Name},
				ScaleIn: &opsv1alpha1.ScaleIn{
					ReplicaChanger: opsv1alpha1.ReplicaChanger{ReplicaChanges: &absReplicas},
				},
			}
		}

		horizontalScalingList = append(horizontalScalingList, scaling)
	}

	specificOps := opsv1alpha1.SpecificOpsRequest{
		HorizontalScalingList: horizontalScalingList,
	}

	_, err := createOpsRequest(ctx, c, params.Cluster, opsv1alpha1.HorizontalScalingType, specificOps)
	return err
}

// CreateVerticalScalingOpsRequest 为指定的 Cluster 创建垂直伸缩 OpsRequest
func CreateVerticalScalingOpsRequest(ctx context.Context,
	c client.Client,
	params model.VerticalScalingOpsParams,
) error {
	var verticalScalingList []opsv1alpha1.VerticalScaling

	// 遍历所有组件，为每个组件创建对应的资源配置
	for _, component := range params.Components {
		verticalScalingList = append(verticalScalingList, opsv1alpha1.VerticalScaling{
			ComponentOps: opsv1alpha1.ComponentOps{ComponentName: component.Name},
			ResourceRequirements: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    component.CPU,
					corev1.ResourceMemory: component.Memory,
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    component.CPU,
					corev1.ResourceMemory: component.Memory,
				},
			},
		})
	}

	specificOps := opsv1alpha1.SpecificOpsRequest{
		VerticalScalingList: verticalScalingList,
	}

	_, err := createOpsRequest(ctx, c, params.Cluster, opsv1alpha1.VerticalScalingType, specificOps)
	return err
}

// CreateVolumeExpansionOpsRequest 为指定的 Cluster 创建存储扩容 OpsRequest
func CreateVolumeExpansionOpsRequest(ctx context.Context,
	c client.Client,
	params model.VolumeExpansionOpsParams,
) error {
	var volumeExpansionList []opsv1alpha1.VolumeExpansion

	// 遍历所有组件，为每个组件创建对应的存储扩容配置
	for _, component := range params.Components {
		volumeExpansionList = append(volumeExpansionList, opsv1alpha1.VolumeExpansion{
			ComponentOps: opsv1alpha1.ComponentOps{ComponentName: component.Name},
			VolumeClaimTemplates: []opsv1alpha1.OpsRequestVolumeClaimTemplate{
				{
					Name:    component.VolumeClaimTemplateName,
					Storage: component.Storage,
				},
			},
		})
	}

	specificOps := opsv1alpha1.SpecificOpsRequest{
		VolumeExpansionList: volumeExpansionList,
	}

	_, err := createOpsRequest(ctx, c, params.Cluster, opsv1alpha1.VolumeExpansionType, specificOps)
	return err
}

// CreateParameterChangeOpsRequest 创建参数变更 OpsRequest
func CreateParameterChangeOpsRequest(ctx context.Context,
	c client.Client,
	cluster *kbappsv1.Cluster,
	parameters []model.ParameterEntry,
) error {
	specificOps := opsv1alpha1.SpecificOpsRequest{
		Reconfigures: []opsv1alpha1.Reconfigure{
			{
				ComponentOps: opsv1alpha1.ComponentOps{ComponentName: ClusterType(cluster)},
			},
		},
	}

	var parameterPairs []opsv1alpha1.ParameterPair
	for _, parameter := range parameters {
		if strValue, ok := parameter.Value.(*string); ok {
			parameterPairs = append(parameterPairs, opsv1alpha1.ParameterPair{
				Key:   parameter.Name,
				Value: strValue,
			})
		}
	}

	specificOps.Reconfigures[0].Parameters = parameterPairs

	_, err := createOpsRequest(ctx, c, cluster, opsv1alpha1.ReconfiguringType, specificOps)
	return err
}

// CreateRestoreOpsRequest 使用 backupName 指定一个 backup 创建 Restore OpsRequest，从备份中恢复 cluster
//
// 通过 backup 恢复的 Cluster 的名称格式为 {cluster.Name(去除四位后缀)}-restore-{四位随机后缀},
// 串行恢复卷声明，在集群进行 running 状态后执行 PostReady
func CreateRestoreOpsRequest(ctx context.Context,
	c client.Client,
	cluster *kbappsv1.Cluster,
	backupName string,
) (*opsv1alpha1.OpsRequest, error) {
	specificOps := opsv1alpha1.SpecificOpsRequest{
		Restore: &opsv1alpha1.Restore{
			BackupName:                        backupName,
			VolumeRestorePolicy:               "Serial",
			DeferPostReadyUntilClusterRunning: true,
		},
	}

	return createOpsRequest(ctx, c, cluster, opsv1alpha1.RestoreType, specificOps)
}

// createOpsRequest 创建 OpsRequest
//
// OpsRequest 的名称格式为 {clustername}-{opsType}-{timestamp}，
// 使用时间戳确保每次操作都有唯一的名称
func createOpsRequest(
	ctx context.Context,
	c client.Client,
	cluster *kbappsv1.Cluster,
	opsType opsv1alpha1.OpsType,
	specificOps opsv1alpha1.SpecificOpsRequest,
	opts ...createOption,
) (*opsv1alpha1.OpsRequest, error) {
	options := applyCreateOptions(opts...)

	ops := buildOpsRequest(cluster, opsType, specificOps)

	res, err := options.preflight.decide(ctx, c, ops)
	if err != nil {
		return nil, fmt.Errorf("preflight check for opsruqest %s failed: %w", ops.Name, err)
	}

	if res.Decision == preflightSkip {
		return nil, ErrCreateOpsSkipped
	}

	if err := c.Create(ctx, ops); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil, ErrCreateOpsSkipped
		}
		return nil, fmt.Errorf("create opsrequest %s: %w", ops.Name, err)
	}

	return ops, nil
}

// buildOpsRequest 构造 OpsRequest 对象
func buildOpsRequest(
	cluster *kbappsv1.Cluster,
	opsType opsv1alpha1.OpsType,
	specificOps opsv1alpha1.SpecificOpsRequest,
) *opsv1alpha1.OpsRequest {
	name := makeOpsRequestName(cluster.Name, opsType)

	serviceID := cluster.GetLabels()[index.ServiceIDLabel]

	labels := map[string]string{
		constant.AppInstanceLabelKey:    cluster.Name,
		constant.OpsRequestTypeLabelKey: string(opsType),
		index.ServiceIDLabel:            serviceID,
	}

	ops := &opsv1alpha1.OpsRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: opsv1alpha1.OpsRequestSpec{
			ClusterName:                           cluster.Name,
			Type:                                  opsType,
			TimeoutSeconds:                        &opsTimeoutSecond,
			TTLSecondsAfterUnsuccessfulCompletion: opsLifeAfterUnsuccess,
			TTLSecondsAfterSucceed:                opsLifeAfterSucceed,

			SpecificOpsRequest: specificOps,
		},
	}

	// 依据 opsType 设置不同的 spec 字段
	switch opsType {
	case opsv1alpha1.RestoreType:
		// Restore 中 ClusterName 为通过备份恢复的 Cluster 的名称，会创建一个新的 Cluster，
		// 应当按照 {cluster.Name(去除后缀)}-restore-{四位随机后缀}" 的格式
		ops.Spec.ClusterName = generateRestoredClusterName(cluster.Name)
	}

	return ops
}

func applyCreateOptions(opts ...createOption) *createOpts {
	o := &createOpts{}
	for _, f := range opts {
		if f != nil {
			f(o)
		}
	}
	applyDefaultCreateOptions(o)
	return o
}

func applyDefaultCreateOptions(o *createOpts) {
	if o.preflight == nil {
		o.preflight = uniqueOps{}
	}
}

// makeOpsRequestName 生成 OpsRequest 名称
// 格式：{clustername}-{opsType}-{timestamp}
func makeOpsRequestName(clusterName string, opsType opsv1alpha1.OpsType) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s-%s-%x", clusterName, strings.ToLower(string(opsType)), timestamp)
}

// getOpsRequestsByIndex 使用索引查询 OpsRequest，失败时回退到标签查询
func getOpsRequestsByIndex(ctx context.Context, c client.Client, namespace, clusterName string, opsType opsv1alpha1.OpsType) ([]opsv1alpha1.OpsRequest, error) {
	var list opsv1alpha1.OpsRequestList

	indexKey := fmt.Sprintf("%s/%s/%s", namespace, clusterName, opsType)
	if err := c.List(ctx, &list, client.MatchingFields{index.NamespaceClusterOpsTypeField: indexKey}); err == nil {
		return list.Items, nil
	}

	if err := c.List(ctx, &list,
		client.InNamespace(namespace),
		client.MatchingLabels(map[string]string{
			constant.AppInstanceLabelKey:    clusterName,
			constant.OpsRequestTypeLabelKey: string(opsType),
		}),
	); err != nil {
		return nil, fmt.Errorf("list opsrequests for preflight: %w", err)
	}

	return list.Items, nil
}

// isOpsRequestInFinalPhase 检查操作请求是否处于终态
func isOpsRequestInFinalPhase(ops *opsv1alpha1.OpsRequest) bool {
	phase := ops.Status.Phase
	return phase == opsv1alpha1.OpsSucceedPhase ||
		phase == opsv1alpha1.OpsCancelledPhase ||
		phase == opsv1alpha1.OpsFailedPhase ||
		phase == opsv1alpha1.OpsAbortedPhase
}

// generateRestoredClusterName 生成 restore cluster 的名称
// 格式：{cluster.Name(去除后缀)}-restore-{四位随机后缀}
func generateRestoredClusterName(originalClusterName string) string {
	var baseName string

	// 避免重复叠加 restore 后缀
	if strings.Contains(originalClusterName, "-restore-") {
		restoreIndex := strings.LastIndex(originalClusterName, "-restore-")
		baseName = originalClusterName[:restoreIndex]
	} else {
		lastDash := strings.LastIndex(originalClusterName, "-")
		baseName = originalClusterName[:lastDash]
	}

	// 生成4位随机后缀
	timestamp := time.Now().UnixNano()
	input := fmt.Sprintf("%s-restore-%d", baseName, timestamp)
	hash := md5.Sum([]byte(input))
	hashSuffix := fmt.Sprintf("%x", hash[:2])

	return fmt.Sprintf("%s-restore-%s", baseName, hashSuffix)
}
