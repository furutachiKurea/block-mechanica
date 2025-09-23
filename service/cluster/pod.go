package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/kbkit"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	workloadsv1 "github.com/apecloud/kubeblocks/apis/workloads/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetPodDetail 获取指定 Cluster 的 Pod detail
// 获取指定 service_id 的 Cluster 管理的指定 Pod 的详细信息
func (s *Service) GetPodDetail(ctx context.Context, serviceID string, podName string) (*model.PodDetail, error) {
	cluster, err := kbkit.GetClusterByServiceID(ctx, s.client, serviceID)
	if err != nil {
		return nil, fmt.Errorf("get cluster by service_id %s: %w", serviceID, err)
	}

	pods, err := s.getClusterPods(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("get cluster pods: %w", err)
	}

	targetPod := findPodByName(pods, podName)
	if targetPod == nil {
		return nil, kbkit.ErrTargetNotFound
	}

	pod := &corev1.Pod{}
	if err := s.client.Get(ctx, client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, pod); err != nil {
		return nil, fmt.Errorf("get pod %s: %w", podName, err)
	}

	var (
		componentName   = pod.Labels["apps.kubeblocks.io/component-name"]
		instanceSetName = pod.Labels["workloads.kubeblocks.io/instance"]
		componentDef    = ""
		version         = ""
	)

	// 同 instanceSet 获取 componentDef
	if instanceSetName != "" {
		var instanceSet workloadsv1.InstanceSet
		if err := s.client.Get(
			ctx, client.ObjectKey{
				Name:      instanceSetName,
				Namespace: cluster.Namespace,
			}, &instanceSet); err != nil {
			log.Warn("Failed to get instanceset for pod",
				log.String("pod", podName),
				log.String("instanceset", instanceSetName),
				log.Err(err))
		} else {
			if componentName == "" {
				componentName = instanceSet.Labels["apps.kubeblocks.io/component-name"]
			}
			if v := instanceSet.Annotations["app.kubernetes.io/component"]; v != "" {
				componentDef = v
			}
		}
	}

	// 通过 componentSpec 获取 version
	if componentName != "" {
		if spec := findComponentSpec(cluster, componentName); spec != nil {
			if componentDef == "" {
				componentDef = spec.ComponentDef
			}
			if spec.ComponentDef != "" {
				version = spec.ComponentDef
			} else if spec.ServiceVersion != "" {
				version = spec.ServiceVersion
			}
		}
	}

	if version == "" && componentDef != "" {
		version = componentDef
	}

	if componentDef == "" && len(cluster.Spec.ComponentSpecs) > 0 {
		fallback := cluster.Spec.ComponentSpecs[0]
		componentDef = fallback.ComponentDef
		if version == "" {
			if fallback.ComponentDef != "" {
				version = fallback.ComponentDef
			} else {
				version = fallback.ServiceVersion
			}
		}
	}

	status := buildPodDetailStatus(*pod)
	containers := buildContainerDetails(pod.Spec.Containers, pod.Status.ContainerStatuses, componentDef)
	events, err := getPodEventsByIndex(ctx, s.client, podName, pod.Namespace)
	if err != nil {
		log.Warn("Failed to get pod events",
			log.String("pod", podName),
			log.String("namespace", pod.Namespace),
			log.Err(err))
		events = []model.PodEvent{}
	}

	startTime := ""
	if pod.Status.StartTime != nil {
		startTime = formatToISO8601Time(pod.Status.StartTime.Time)
	}

	podDetail := &model.PodDetail{
		Name:       pod.Name,
		NodeIP:     pod.Status.HostIP,
		StartTime:  startTime,
		IP:         pod.Status.PodIP,
		Version:    version,
		Namespace:  pod.Namespace,
		Status:     status,
		Containers: containers,
		Events:     events,
	}

	log.Debug("get pod detail",
		log.String("service_id", serviceID),
		log.String("pod", podName),
		log.Any("detail", podDetail))

	return podDetail, nil
}

// findPodByName 在 Pod 状态列表中查找指定名称的 Pod
func findPodByName(pods []model.Status, podName string) *model.Status {
	for _, pod := range pods {
		if pod.Name == podName {
			return &pod
		}
	}
	return nil
}

func findComponentSpec(cluster *kbappsv1.Cluster, componentName string) *kbappsv1.ClusterComponentSpec {
	if cluster == nil || componentName == "" {
		return nil
	}
	for i := range cluster.Spec.ComponentSpecs {
		if cluster.Spec.ComponentSpecs[i].Name == componentName {
			return &cluster.Spec.ComponentSpecs[i]
		}
	}
	return nil
}

// buildPodDetailStatus 构建符合注释约定的 PodStatus（包含 type_str/reason/message/advice）
func buildPodDetailStatus(pod corev1.Pod) model.PodStatus {
	typeStr := strings.ToLower(string(pod.Status.Phase))
	reason := ""
	message := ""
	advice := ""

	// 优先取 Waiting 的容器状态
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			reason = cs.State.Waiting.Reason
			message = cs.State.Waiting.Message
			advice = deriveAdvice(reason, message)
			break
		}
	}
	// 其次取 Terminated 的容器状态
	if reason == "" {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				reason = cs.State.Terminated.Reason
				message = cs.State.Terminated.Message
				advice = deriveAdvice(reason, message)
				break
			}
		}
	}

	return model.PodStatus{
		TypeStr: typeStr,
		Reason:  reason,
		Message: message,
		Advice:  advice,
	}
}

// buildContainerDetails 构建容器详情列表，只返回设置了资源限制的工作容器
func buildContainerDetails(containers []corev1.Container, containerStatuses []corev1.ContainerStatus, componentDef string) []model.Container {
	var details []model.Container

	statusMap := make(map[string]corev1.ContainerStatus)
	for _, status := range containerStatuses {
		statusMap[status.Name] = status
	}

	for _, container := range containers {
		if !hasResourceLimits(container.Resources.Limits) {
			continue
		}

		status, exists := statusMap[container.Name]
		if !exists {
			continue
		}

		startedTime := ""
		state := "Unknown"
		reason := ""

		if status.State.Running != nil {
			startedTime = formatToISO8601Time(status.State.Running.StartedAt.Time)
			state = "Running"
		} else if status.State.Waiting != nil {
			state = "Waiting"
			reason = status.State.Waiting.Reason
		} else if status.State.Terminated != nil {
			state = "Terminated"
			reason = status.State.Terminated.Reason
		}

		limitCPU := ""
		if cpu := container.Resources.Limits.Cpu(); cpu != nil {
			limitCPU = cpu.String()
		}

		limitMemory := ""
		if memory := container.Resources.Limits.Memory(); memory != nil {
			limitMemory = memory.String()
		}

		containerDetail := model.Container{
			ComponentDef: componentDef,
			LimitMemory:  limitMemory,
			LimitCPU:     limitCPU,
			Started:      startedTime,
			State:        state,
			Reason:       reason,
		}

		details = append(details, containerDetail)
	}

	return details
}

// deriveAdvice 将常见的 reason 映射为建议性结论
func deriveAdvice(reason, message string) string {
	switch reason {
	case "OOMKilled":
		return "OutOfMemory"
	case "ImagePullBackOff", "ErrImagePull":
		return "ImagePullError"
	default:
		_ = message
		return ""
	}
}

// getPodEventsByIndex 使用索引查询 Pod 相关的 Event
func getPodEventsByIndex(ctx context.Context, c client.Client, podName, namespace string) ([]model.PodEvent, error) {
	var eventList corev1.EventList

	indexKey := fmt.Sprintf("%s/%s", namespace, podName)
	if err := c.List(ctx, &eventList, client.MatchingFields{index.NamespacePodNameField: indexKey}); err != nil {
		log.Warn("Index query for pod events failed",
			log.String("indexKey", indexKey),
			log.String("pod", podName),
			log.String("namespace", namespace),
			log.Err(err))
		return []model.PodEvent{}, nil
	}

	return processEvents(eventList.Items), nil
}

// processEvents 处理 Event 列表
func processEvents(events []corev1.Event) []model.PodEvent {
	// 按时间排序
	sort.Slice(events, func(i, j int) bool {
		return events[i].FirstTimestamp.After(events[j].FirstTimestamp.Time)
	})

	// 限制返回数量
	const maxEvents = 10
	endIndex := len(events)
	if endIndex > maxEvents {
		endIndex = maxEvents
	}

	result := make([]model.PodEvent, 0, endIndex)
	for i := 0; i < endIndex; i++ {
		event := events[i]
		result = append(result, model.PodEvent{
			Type:    event.Type,
			Reason:  event.Reason,
			Age:     formatAge(event.FirstTimestamp),
			Message: event.Message,
		})
	}

	return result
}

// formatAge 将时间差格式化为人类可读的格式 (如 "5m", "2h", "3d")
func formatAge(eventTime metav1.Time) string {
	if eventTime.IsZero() {
		return ""
	}

	duration := time.Since(eventTime.Time)

	if duration < time.Minute {
		return fmt.Sprintf("%.0fs", duration.Seconds())
	} else if duration < time.Hour {
		return fmt.Sprintf("%.0fm", duration.Minutes())
	} else if duration < 24*time.Hour {
		return fmt.Sprintf("%.0fh", duration.Hours())
	} else {
		return fmt.Sprintf("%.0fd", duration.Hours()/24)
	}
}

// hasResourceLimits 检查是否设置了 CPU 或 Memory 资源限制
func hasResourceLimits(limits corev1.ResourceList) bool {
	if limits == nil {
		return false
	}

	cpu, hasCPU := limits[corev1.ResourceCPU]
	memory, hasMemory := limits[corev1.ResourceMemory]

	return (hasCPU && !cpu.IsZero()) || (hasMemory && !memory.IsZero())
}
