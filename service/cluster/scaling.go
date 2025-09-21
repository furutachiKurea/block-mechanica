package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/service/kbkit"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ExpansionCluster 对 Cluster 进行伸缩操作
//
// 使用 opsrequest 将 Cluster 的资源规格进行伸缩，使其变为 model.ExpansionInput 的期望状态
func (s *Service) ExpansionCluster(ctx context.Context, expansion model.ExpansionInput) error {
	log.Debug("Expansion",
		log.String("service_id", expansion.ServiceID),
		log.Any("expansion", expansion),
	)

	cluster, err := kbkit.GetClusterByServiceID(ctx, s.client, expansion.ServiceID)
	if err != nil {
		return err
	}
	if len(cluster.Spec.ComponentSpecs) == 0 {
		return fmt.Errorf("cluster %s/%s has no componentSpecs", cluster.Namespace, cluster.Name)
	}

	component := cluster.Spec.ComponentSpecs[0]
	componentName := component.Name
	if componentName == "" {
		componentName = cluster.Spec.ClusterDef
	}

	desiredCPU, err := resource.ParseQuantity(expansion.CPU)
	if err != nil {
		return fmt.Errorf("parse desired cpu %q: %w", expansion.CPU, err)
	}
	desiredMem, err := resource.ParseQuantity(expansion.Memory)
	if err != nil {
		return fmt.Errorf("parse desired memory %q: %w", expansion.Memory, err)
	}
	desiredStorage, err := resource.ParseQuantity(expansion.Storage)
	if err != nil {
		return fmt.Errorf("parse desired storage %q: %w", expansion.Storage, err)
	}

	currentCPU := component.Resources.Limits.Cpu()
	currentMem := component.Resources.Limits.Memory()
	var (
		hasPVC          = len(component.VolumeClaimTemplates) > 0
		volumeTplName   string
		currentStorage  resource.Quantity
		storageClassRef *string
	)
	if hasPVC {
		volumeTpl := component.VolumeClaimTemplates[0]
		volumeTplName = volumeTpl.Name
		if size, ok := volumeTpl.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			currentStorage = size
		}
		storageClassRef = volumeTpl.Spec.StorageClassName
	}

	var opsCreated bool

	expansionCtx := model.ExpansionContext{
		Cluster:       cluster,
		ComponentName: componentName,
		// 水平伸缩
		CurrentReplicas: component.Replicas,
		DesiredReplicas: expansion.Replicas,
		// 垂直伸缩
		CurrentCPU: *currentCPU,
		CurrentMem: *currentMem,
		DesiredCPU: desiredCPU,
		DesiredMem: desiredMem,
		// 存储扩容
		HasPVC:          hasPVC,
		VolumeTplName:   volumeTplName,
		CurrentStorage:  currentStorage,
		DesiredStorage:  desiredStorage,
		StorageClassRef: storageClassRef,
	}

	hCreated, err := s.handleHorizontalScaling(ctx, expansionCtx)
	if err != nil {
		return fmt.Errorf("horizontal scaling: %w", err)
	}
	opsCreated = opsCreated || hCreated

	vCreated, err := s.handleVerticalScaling(ctx, expansionCtx)
	if err != nil {
		return fmt.Errorf("vertical scaling: %w", err)
	}
	opsCreated = opsCreated || vCreated

	sCreated, err := s.handleVolumeExpansion(ctx, expansionCtx)
	if err != nil {
		return fmt.Errorf("volume expansion: %w", err)
	}
	opsCreated = opsCreated || sCreated

	if !opsCreated {
		log.Info("No expansion needed, cluster already matches desired spec",
			log.String("cluster", cluster.Name),
			log.String("service_id", expansion.ServiceID))
	}

	return nil
}

// handleHorizontalScaling 处理水平伸缩（副本数）
func (s *Service) handleHorizontalScaling(ctx context.Context, scalingCtx model.ExpansionContext) (bool, error) {
	if scalingCtx.DesiredReplicas == scalingCtx.CurrentReplicas {
		return false, nil
	}

	delta := scalingCtx.DesiredReplicas - scalingCtx.CurrentReplicas

	opsParams := model.HorizontalScalingOpsParams{
		Cluster:       scalingCtx.Cluster,
		ComponentName: scalingCtx.ComponentName,
		DeltaReplicas: delta,
	}

	if err := kbkit.CreateHorizontalScalingOpsRequest(ctx, s.client, opsParams); err != nil {
		if errors.Is(err, kbkit.ErrCreateOpsSkipped) {
			return false, nil
		}
		return false, fmt.Errorf("create horizontal scaling opsrequest: %w", err)
	}

	log.Info("Created horizontal scaling OpsRequest",
		log.String("cluster", scalingCtx.Cluster.Name),
		log.String("component", scalingCtx.ComponentName),
		log.Int32("deltaReplicas", delta))

	return true, nil
}

// handleVerticalScaling 处理垂直伸缩（CPU/内存）
func (s *Service) handleVerticalScaling(ctx context.Context, scalingCtx model.ExpansionContext) (bool, error) {
	needVScale := scalingCtx.CurrentCPU.Cmp(scalingCtx.DesiredCPU) != 0 ||
		scalingCtx.CurrentMem.Cmp(scalingCtx.DesiredMem) != 0

	if !needVScale {
		return false, nil
	}

	opsParams := model.VerticalScalingOpsParams{
		Cluster:       scalingCtx.Cluster,
		ComponentName: scalingCtx.ComponentName,
		CPU:           scalingCtx.DesiredCPU,
		Memory:        scalingCtx.DesiredMem,
	}

	if err := kbkit.CreateVerticalScalingOpsRequest(ctx, s.client, opsParams); err != nil {
		if errors.Is(err, kbkit.ErrCreateOpsSkipped) {
			return false, nil
		}
		return false, fmt.Errorf("create vertical scaling opsrequest: %w", err)
	}

	log.Info("Created vertical scaling OpsRequest",
		log.String("cluster", scalingCtx.Cluster.Name),
		log.String("component", scalingCtx.ComponentName),
		log.String("desiredCPU", scalingCtx.DesiredCPU.String()),
		log.String("desiredMemory", scalingCtx.DesiredMem.String()))

	return true, nil
}

// handleVolumeExpansion 处理存储扩容
func (s *Service) handleVolumeExpansion(ctx context.Context, scalingCtx model.ExpansionContext) (bool, error) {
	if !scalingCtx.HasPVC {
		return false, nil
	}

	switch scalingCtx.DesiredStorage.Cmp(scalingCtx.CurrentStorage) {
	case 0:
		return false, nil
	case -1:
		log.Warn("Storage shrinking detected but not supported, skipping operation",
			log.String("cluster", scalingCtx.Cluster.Name),
			log.String("component", scalingCtx.ComponentName),
			log.String("volumeTemplate", scalingCtx.VolumeTplName),
			log.String("currentStorage", scalingCtx.CurrentStorage.String()),
			log.String("desiredStorage", scalingCtx.DesiredStorage.String()))
		return false, nil
	case 1:
		canExpand := true
		var skipReason string

		if scalingCtx.StorageClassRef == nil || *scalingCtx.StorageClassRef == "" {
			canExpand = false
			skipReason = "storageClass not set on volumeClaimTemplate"
		} else {
			var sc storagev1.StorageClass
			if err := s.client.Get(ctx, client.ObjectKey{Name: *scalingCtx.StorageClassRef}, &sc); err != nil {
				log.Warn("Failed to get StorageClass, skipping volume expansion",
					log.String("cluster", scalingCtx.Cluster.Name),
					log.String("component", scalingCtx.ComponentName),
					log.String("volumeTemplate", scalingCtx.VolumeTplName),
					log.String("storageClass", *scalingCtx.StorageClassRef),
					log.String("error", err.Error()))
				canExpand = false
				skipReason = "failed to get StorageClass"
			} else if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
				canExpand = false
				skipReason = "StorageClass does not allow volume expansion"
			}
		}

		if !canExpand {
			log.Warn("Volume expansion skipped due to configuration constraints",
				log.String("cluster", scalingCtx.Cluster.Name),
				log.String("component", scalingCtx.ComponentName),
				log.String("volumeTemplate", scalingCtx.VolumeTplName),
				log.String("reason", skipReason),
				log.String("currentStorage", scalingCtx.CurrentStorage.String()),
				log.String("desiredStorage", scalingCtx.DesiredStorage.String()))
			return false, nil
		}

		opsParams := model.VolumeExpansionOpsParams{
			Cluster:                 scalingCtx.Cluster,
			ComponentName:           scalingCtx.ComponentName,
			VolumeClaimTemplateName: scalingCtx.VolumeTplName,
			Storage:                 scalingCtx.DesiredStorage,
		}

		if err := kbkit.CreateVolumeExpansionOpsRequest(ctx, s.client, opsParams); err != nil {
			if errors.Is(err, kbkit.ErrCreateOpsSkipped) {
				return false, nil
			}
			return false, fmt.Errorf("create volume expansion opsrequest: %w", err)
		}

		log.Info("Created volume expansion OpsRequest",
			log.String("cluster", scalingCtx.Cluster.Name),
			log.String("component", scalingCtx.ComponentName),
			log.String("volumeTemplate", scalingCtx.VolumeTplName),
			log.String("desiredStorage", scalingCtx.DesiredStorage.String()))
		return true, nil
	}

	return false, nil
}
