package service

// resource.go 提供集群资源的相关操作
// TODO 拆分文件或者分包

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/furutachiKurea/block-mechanica/internal/index"
	"github.com/furutachiKurea/block-mechanica/internal/log"
	"github.com/furutachiKurea/block-mechanica/internal/model"
	"github.com/furutachiKurea/block-mechanica/internal/mono"

	kbappsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	parametersv1alpha1 "github.com/apecloud/kubeblocks/apis/parameters/v1alpha1"
	"github.com/sahilm/fuzzy"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// SupersededByRestoreAnnotation 标记已被备份恢复替代的旧 Cluster
	// 当从备份恢复创建新cluster后，原cluster会被标记此annotation以避免查询冲突
	// getClusterByServiceID 会自动过滤掉带有此annotation的cluster
	SupersededByRestoreAnnotation = "block-mechanica.rainbond.io/superseded-by-restore"
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

// GetClusterParameter 获取 KubeBlocks Cluster 的 Parameter
func (s *ResourceService) GetClusterParameter(ctx context.Context, query model.ClusterParametersQuery) (*model.PaginatedResult[model.Parameter], error) {
	// 先通过 ComponentDefinition 获取 Parameters(value 为 definition 中的默认值)，
	// 再通过 configmap 从数据库配置文件构造 ParameterEntry，
	// 最后将获取到的 Parameters 与 ParameterEntry 取交集，确保只返回数据库配置文件中的 Parameter

	query.Validate()

	cluster, err := getClusterByServiceID(ctx, s.client, query.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("get cluster by service_id %s: %w", query.ServiceID, err)
	}

	var (
		constraints      map[string]model.Parameter
		parameterEntries []model.ParameterEntry
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c, err := s.getParameterConstraints(gctx, cluster)
		if err != nil {
			return fmt.Errorf("get parameter constraints: %w", err)
		}
		constraints = c
		return nil
	})

	g.Go(func() error {
		pe, err := s.getParametersFromConfigmap(gctx, cluster)
		if err != nil {
			return fmt.Errorf("get parameters from ConfigMap: %w", err)
		}
		parameterEntries = pe
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	parameters := mergeEntriesAndConstraints(parameterEntries, constraints)

	// Rainbond 隐藏 immutable 参数
	parameters = filterOutImmutableParameters(parameters)

	// 对参数名称进行搜索
	if keyword := strings.TrimSpace(query.Keyword); keyword != "" {
		parameters = filterParametersByKeyword(parameters, keyword)
	}

	slices.SortStableFunc(parameters, func(a, b model.Parameter) int {
		return cmp.Compare(a.Name, b.Name)
	})

	totalCount := len(parameters)
	result := paginate(parameters, query.Page, query.PageSize)

	log.Debug("get paginated parameters", log.Any("parameters", parameters))
	return &model.PaginatedResult[model.Parameter]{
		Items: result,
		Total: totalCount,
	}, nil
}

// ChangeClusterParameter 变更给定 service_id 对应的 Cluster 的参数设置
func (s *ResourceService) ChangeClusterParameter(
	ctx context.Context,
	req model.ClusterParametersChange,
) (*model.ParameterChangeResult, error) {
	cluster, err := getClusterByServiceID(ctx, s.client, req.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("get cluster by service_id %s: %w", req.ServiceID, err)
	}

	constraints, err := s.getParameterConstraints(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("get parameter constraints: %w", err)
	}

	// 将约束转换为参数列表以创建验证器
	constraintList := make([]model.Parameter, 0, len(constraints))
	for _, constraint := range constraints {
		constraintList = append(constraintList, constraint)
	}

	// 创建参数验证器
	validator := NewParameterValidator(constraintList)

	paramCount := len(req.Parameters)
	applied := make([]string, 0, paramCount)                        // 成功应用的参数名称列表
	invalids := make([]model.ParameterChangeError, 0, paramCount/4) // 校验失败的参数，预期25%失败率
	validParameters := make([]model.ParameterEntry, 0, paramCount)  // 符合约束用于创建 Ops 的参数

	// 验证所有参数变更
	for _, parameterToChange := range req.Parameters {
		if validationErr := validator.Validate(parameterToChange); validationErr != nil {
			// 验证失败，添加到 invalid
			invalids = append(invalids, model.ParameterChangeError{
				Name: validationErr.ParameterName,
				Code: string(validationErr.ErrorCode),
			})
			continue
		}

		// 验证成功
		applied = append(applied, parameterToChange.Name)

		// 构建用于创建 OpsRequest 的参数
		validParam := model.ParameterEntry{
			Name:  parameterToChange.Name,
			Value: validator.ConvertToStringValue(parameterToChange.Value),
		}
		validParameters = append(validParameters, validParam)
	}

	// 创建 OpsRequest（仅当存在有效参数变更时）
	if len(validParameters) > 0 {
		if err := createParameterChangeOpsRequest(ctx, s.client, cluster, validParameters); err != nil {
			return nil, fmt.Errorf("create parameter change OpsRequest: %w", err)
		}

		log.Debug("created parameter change OpsRequest",
			log.String("clusterName", cluster.Name),
			log.Int("parameterCount", len(validParameters)))
	}

	result := &model.ParameterChangeResult{
		Applied:  applied,
		Invalids: invalids,
	}

	log.Debug("parameter change operation completed",
		log.String("clusterName", cluster.Name),
		log.Int("appliedCount", len(applied)),
		log.Int("invalidCount", len(invalids)))

	return result, nil
}

// getParameterConstraints 从 KubeBlocks 的参数定义中提取参数约束为 map[string]model.Parameter
// 返回 dynamic、static 与 immutable；componentName 可选，未提供则回退第一个普通组件
// 返回 map[string]model.Parameter，便于后续合并参数约束
func (s *ResourceService) getParameterConstraints(
	ctx context.Context,
	cluster *kbappsv1.Cluster,
	componentName ...string,
) (map[string]model.Parameter, error) {
	compName, err := s.determineComponentName(cluster, componentName...)
	if err != nil {
		return nil, fmt.Errorf("determine component name: %w", err)
	}

	compDef, err := s.resolveComponentDefinition(ctx, cluster, compName)
	if err != nil {
		return nil, fmt.Errorf("resolve component definition: %w", err)
	}

	renderer, err := s.findParamConfigRenderer(ctx, compDef)
	if err != nil {
		return nil, fmt.Errorf("find param config renderer: %w", err)
	}

	paramDefs, err := s.getParameterDefinitions(ctx, renderer)
	if err != nil {
		return nil, fmt.Errorf("get parameter definitions: %w", err)
	}

	parameters := make(map[string]model.Parameter)

	for _, pd := range paramDefs {
		if pd == nil {
			continue
		}
		schema, err := s.processParameterSchema(&pd.Spec)
		if err != nil {
			return nil, fmt.Errorf("process parameter schema: %w", err)
		}
		if schema == nil {
			continue
		}

		properties := s.extractSchemaProperties(schema)
		if len(properties) == 0 {
			continue
		}

		paramSets := createParameterSets(&pd.Spec)

		for paramName, property := range properties {
			param := s.buildParameterConstraint(paramName, property, paramSets)
			if _, exists := parameters[paramName]; exists {
				// 若存在同名参数，后写覆盖先写；记录 Debug 便于追溯
				log.Debug("duplicate parameter name detected; overriding previous entry", log.String("param", paramName))
			}
			parameters[paramName] = param
		}
	}

	return parameters, nil
}

// getParametersFromConfigmap 从 configmap 中获取实际设置的 Parameter 并覆盖默认值
func (s *ResourceService) getParametersFromConfigmap(
	ctx context.Context,
	cluster *kbappsv1.Cluster,
) (parameters []model.ParameterEntry, err error) {

	// 获取对应数据库类型的适配器
	a, exists := _clusterRegistry[cluster.Spec.ClusterDef]
	if !exists {
		return nil, fmt.Errorf("unsupported cluster type: %s", cluster.Spec.ClusterDef)
	}

	// 获取存有参数配置的 ConfigMap 名称
	cmName := a.Coordinator.GetParametersConfigMap(cluster.Name)
	if cmName == nil {
		log.Debug("cluster type does not support parameter configuration", log.String("clusterType", cluster.Spec.ClusterDef))
		return []model.ParameterEntry{}, nil
	}

	// 获取 ConfigMap
	var configMap corev1.ConfigMap
	cmKey := client.ObjectKey{
		Name:      *cmName,
		Namespace: cluster.Namespace,
	}

	if err := s.client.Get(ctx, cmKey, &configMap); err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Debug("parameters ConfigMap not found", log.String("configMap", *cmName), log.String("namespace", cluster.Namespace))
			return []model.ParameterEntry{}, nil
		}
		return nil, fmt.Errorf("get parameters ConfigMap %s: %w", *cmName, err)
	}

	// 使用对应的 Coordinator 解析配置
	parameters, err = a.Coordinator.ParseParameters(configMap.Data)
	if err != nil {
		return nil, fmt.Errorf("parse parameters from ConfigMap %s: %w", *cmName, err)
	}

	log.Debug("successfully loaded parameters from ConfigMap",
		log.String("configMap", *cmName),
		log.String("clusterType", cluster.Spec.ClusterDef),
		log.Int("parameterCount", len(parameters)))

	return parameters, nil
}

// determineComponentName 返回要解析的组件名：
// 优先使用显式传入的 componentName，否则回退到第一个组件；未找到时报错
func (s *ResourceService) determineComponentName(cluster *kbappsv1.Cluster, componentName ...string) (string, error) {
	if len(componentName) > 0 && componentName[0] != "" {
		return componentName[0], nil
	}

	if len(cluster.Spec.ComponentSpecs) == 0 {
		return "", ErrTargetNotFound
	}

	firstCompSpec := cluster.Spec.ComponentSpecs[0]
	if firstCompSpec.Name == "" {
		return "", fmt.Errorf("first component spec has empty name")
	}

	return firstCompSpec.Name, nil
}

// resolveComponentDefinition 根据组件名读取 ComponentDefinition：
func (s *ResourceService) resolveComponentDefinition(ctx context.Context, cluster *kbappsv1.Cluster, componentName string) (*kbappsv1.ComponentDefinition, error) {
	var compSpec *kbappsv1.ClusterComponentSpec
	for i := range cluster.Spec.ComponentSpecs {
		if cluster.Spec.ComponentSpecs[i].Name == componentName {
			compSpec = &cluster.Spec.ComponentSpecs[i]
			break
		}
	}

	if compSpec == nil {
		return nil, fmt.Errorf("component %s not found in cluster: %w", componentName, ErrTargetNotFound)
	}

	if compSpec.ComponentDef == "" {
		return nil, fmt.Errorf("component %s has empty ComponentDef: %w", componentName, ErrTargetNotFound)
	}

	var compDef kbappsv1.ComponentDefinition
	key := client.ObjectKey{Name: compSpec.ComponentDef}
	if err := s.client.Get(ctx, key, &compDef); err != nil {
		return nil, fmt.Errorf("get component definition %s: %w", compSpec.ComponentDef, err)
	}

	return &compDef, nil
}

// findParamConfigRenderer 查找唯一匹配的 ParamConfigRenderer：
// 组件名需匹配，ServiceVersion 为空或等于 compDef 的版本;
// 数量为 0 返回 ErrTargetNotFound，>1 报错
func (s *ResourceService) findParamConfigRenderer(
	ctx context.Context,
	compDef *kbappsv1.ComponentDefinition,
) (*parametersv1alpha1.ParamConfigRenderer, error) {
	var rendererList parametersv1alpha1.ParamConfigRendererList
	if err := s.client.List(ctx, &rendererList); err != nil {
		return nil, fmt.Errorf("list ParamConfigRenderer: %w", err)
	}

	var matchedRenderers []*parametersv1alpha1.ParamConfigRenderer
	for i := range rendererList.Items {
		renderer := &rendererList.Items[i]

		if renderer.Spec.ComponentDef != compDef.Name {
			continue
		}

		rendererServiceVersion := renderer.Spec.ServiceVersion
		compDefServiceVersion := compDef.Spec.ServiceVersion

		if rendererServiceVersion != "" && rendererServiceVersion != compDefServiceVersion {
			continue
		}

		matchedRenderers = append(matchedRenderers, renderer)
	}

	switch len(matchedRenderers) {
	case 0:
		return nil, ErrTargetNotFound
	case 1:
		return matchedRenderers[0], nil
	default:
		return nil, ErrMultipleFounded
	}
}

// getParameterDefinitions 按 renderer.Spec.ParametersDefs 批量获取 ParametersDefinition。
func (s *ResourceService) getParameterDefinitions(
	ctx context.Context,
	renderer *parametersv1alpha1.ParamConfigRenderer,
) ([]*parametersv1alpha1.ParametersDefinition, error) {
	if renderer == nil {
		return nil, nil
	}

	// 在现行体系下，ParametersDefinition 与 ParamConfigRenderer 是一对一的
	paramDefs := make([]*parametersv1alpha1.ParametersDefinition, 0, len(renderer.Spec.ParametersDefs))
	for _, paramDefName := range renderer.Spec.ParametersDefs {
		var paramDef parametersv1alpha1.ParametersDefinition
		key := client.ObjectKey{
			Name: paramDefName,
		}

		if err := s.client.Get(ctx, key, &paramDef); err != nil {
			return nil, fmt.Errorf("get ParametersDefinition %s: %w", paramDefName, err)
		}

		paramDefs = append(paramDefs, &paramDef)
	}

	return paramDefs, nil
}

// processParameterSchema 返回 ParametersDefinition 中的 JSON Schema：
// 仅处理 schemaInJSON，忽略 CUE, 目前的需求下 ParametersDefinition 都支持 spec.parametersSchema.schemaInJSON。
func (s *ResourceService) processParameterSchema(
	spec *parametersv1alpha1.ParametersDefinitionSpec,
) (*apiextensionsv1.JSONSchemaProps, error) {
	if spec.ParametersSchema == nil {
		return nil, nil
	}

	schema := spec.ParametersSchema
	if schema.SchemaInJSON == nil {
		return nil, nil
	}

	return schema.SchemaInJSON, nil
}

// extractSchemaProperties 从 schema.Properties["spec"] 提取一层参数属性；
// 跳过 type==object 的容器字段，返回 name->property 映射。
func (s *ResourceService) extractSchemaProperties(
	schema *apiextensionsv1.JSONSchemaProps,
) map[string]apiextensionsv1.JSONSchemaProps {
	if schema == nil {
		return nil
	}

	if schema.Properties == nil {
		return nil
	}

	specProperty, exists := schema.Properties["spec"]
	if !exists {
		return nil
	}

	if specProperty.Properties == nil {
		return nil
	}

	result := make(map[string]apiextensionsv1.JSONSchemaProps)
	for name, property := range specProperty.Properties {
		if property.Type == "object" {
			continue
		}

		result[name] = property
	}

	return result
}

// createParameterSets 将 ParametersDefinition 中的参数列表转换为集合。
func createParameterSets(spec *parametersv1alpha1.ParametersDefinitionSpec) *model.ParameterSets {
	if spec == nil {
		return &model.ParameterSets{}
	}

	return &model.ParameterSets{
		Static:    sliceToSet(spec.StaticParameters),
		Dynamic:   sliceToSet(spec.DynamicParameters),
		Immutable: sliceToSet(spec.ImmutableParameters),
	}
}

// mergeEntriesAndConstraints 合并 ParameterEntry 与 Parameter
// 仅返回 entries 与 constraints 的交集：
// - 如果某个 entry 未在 constraints 中出现，则跳过
func mergeEntriesAndConstraints(
	entries []model.ParameterEntry,
	constraints map[string]model.Parameter,
) []model.Parameter {
	parameters := make([]model.Parameter, 0, len(entries))
	for _, e := range entries {
		constraint, ok := constraints[e.Name]
		if !ok {
			continue
		}
		param := model.Parameter{
			ParameterEntry: e,
			Type:           constraint.Type,
			MinValue:       constraint.MinValue,
			MaxValue:       constraint.MaxValue,
			EnumValues:     constraint.EnumValues,
			Description:    constraint.Description,
			IsDynamic:      constraint.IsDynamic,
			IsRequired:     constraint.IsRequired,
			IsImmutable:    constraint.IsImmutable,
		}

		// 如果约束中没有类型信息，尝试从参数值推断类型
		if param.Type == "" {
			param.Type = inferParameterType(e.Value)
		}

		parameters = append(parameters, param)
	}

	return parameters
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

// sliceToSet 将字符串切片转换为集合。
func sliceToSet(slice []string) map[string]bool {
	set := make(map[string]bool, len(slice))
	for _, item := range slice {
		set[item] = true
	}
	return set
}

// isDynamicParameter 判定参数是否为动态：
func (s *ResourceService) isDynamicParameter(name string, sets *model.ParameterSets) bool {
	return sets.Dynamic[name]
}

// buildParameterConstraint 构造参数约束：
// Type 优先使用 format；填充描述、动态标记、默认值、数值范围与枚举。
func (s *ResourceService) buildParameterConstraint(name string, property apiextensionsv1.JSONSchemaProps, sets *model.ParameterSets) model.Parameter {
	pType := property.Type
	if strings.TrimSpace(property.Format) != "" {
		pType = property.Format
	}

	parameter := model.Parameter{
		ParameterEntry: model.ParameterEntry{
			Name:  name,
			Value: nil,
		},
		Type:        model.ParameterType(pType),
		Description: strings.TrimSpace(property.Description),
		IsDynamic:   s.isDynamicParameter(name, sets),
		IsRequired:  false,
		IsImmutable: sets.Immutable[name],
	}

	if property.Default != nil && len(property.Default.Raw) > 0 {
		var val any
		if err := json.Unmarshal(property.Default.Raw, &val); err != nil {
			log.Warn("decode default value failed", log.String("param", name), log.Err(err))
		} else {
			parameter.Value = val
		}
	}

	if property.Minimum != nil {
		parameter.MinValue = property.Minimum
	}
	if property.Maximum != nil {
		parameter.MaxValue = property.Maximum
	}

	if len(property.Enum) > 0 {
		enums := make([]string, 0, len(property.Enum))
		for i := range property.Enum {
			if len(property.Enum[i].Raw) == 0 {
				continue
			}
			// 与 kbcli 保持一致：枚举项以 JSON 字符串形式存储（字符串包含引号，布尔/数字为原样 JSON）
			enums = append(enums, string(property.Enum[i].Raw))
		}
		if len(enums) > 0 {
			parameter.EnumValues = enums
		}
	}

	return parameter
}

// filterSupportedAddons mono.Filter 的过滤函数
// 仅返回在 _clusterRegistry 中声明过的数据库类型，确保返回值与系统实际可创建的类型一致。
// 判定是否受 Block Mechanica 支持时, 不同 toplogy 的 addon 视为同一类型
func filterSupportedAddons(addon *model.Addon) bool {
	t := addon.Type
	// TODO
	/* if i := strings.LastIndex(t, "-"); i > 0 {
	    t = t[:i]
	} */
	_, ok := _clusterRegistry[t]
	return ok
}

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
		return []T{}
	}

	offset := (page - 1) * pageSize
	if offset >= len(items) {
		return []T{}
	}

	end := min(offset+pageSize, len(items))
	return items[offset:end]
}

// filterParametersByKeyword 对参数列表进行关键词搜索过滤, 匹配参数名称和描述
func filterParametersByKeyword(parameters []model.Parameter, keyword string) []model.Parameter {
	if strings.TrimSpace(keyword) == "" {
		return parameters
	}

	keyword = strings.TrimSpace(keyword)
	var result []model.Parameter

	for _, param := range parameters {
		// 使用 fuzzy 搜索检查参数名称是否匹配
		nameMatches := fuzzy.Find(keyword, []string{param.Name})
		if len(nameMatches) > 0 {
			result = append(result, param)
			continue
		}
	}

	return result
}

// filterOutImmutableParameters 过滤掉不可变参数
func filterOutImmutableParameters(parameters []model.Parameter) []model.Parameter {
	if len(parameters) == 0 {
		return parameters
	}
	result := make([]model.Parameter, 0, len(parameters))
	for _, p := range parameters {
		if p.IsImmutable {
			continue
		}
		result = append(result, p)
	}
	return result
}
