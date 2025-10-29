package kbkit

import (
	"testing"

	"github.com/furutachiKurea/kb-adapter-rbdplugin/internal/model"
	"github.com/furutachiKurea/kb-adapter-rbdplugin/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestParameterValidator_Validate_ParameterWithoutSchema(t *testing.T) {
	tests := []struct {
		name        string
		constraints []model.Parameter
		entry       model.ParameterEntry
		expectError bool
		errorCode   ParameterErrCode
		description string
	}{
		{
			name: "parameter_without_schema_should_pass_validation",
			constraints: []model.Parameter{
				{
					ParameterEntry: model.ParameterEntry{
						Name:  "lower_case_table_names",
						Value: nil,
					},
					Type:        "", // 空类型表示无 schema 定义
					IsDynamic:   false,
					IsImmutable: false,
				},
			},
			entry:       testutil.NewParameterEntry("lower_case_table_names", "1"),
			expectError: false,
			description: "无 schema 定义的参数应该跳过类型校验，允许修改",
		},
		{
			name: "immutable_parameter_without_schema_should_be_rejected",
			constraints: []model.Parameter{
				{
					ParameterEntry: model.ParameterEntry{
						Name:  "immutable_param",
						Value: nil,
					},
					Type:        "", // 空类型
					IsDynamic:   false,
					IsImmutable: true, // 但是标记为不可变
				},
			},
			entry:       testutil.NewParameterEntry("immutable_param", "value"),
			expectError: true,
			errorCode:   ParamImmutable,
			description: "即使无 schema，immutable 参数仍应被拒绝",
		},
		{
			name: "parameter_with_schema_should_validate_type",
			constraints: []model.Parameter{
				testutil.NewParameterConstraint("max_connections").
					WithType(model.ParameterTypeInteger).
					Build(),
			},
			entry:       testutil.NewParameterEntry("max_connections", "not_a_number"),
			expectError: true,
			errorCode:   ParamInvalidType,
			description: "有 schema 定义的参数应正常进行类型校验",
		},
		{
			name: "parameter_without_schema_accepts_any_type",
			constraints: []model.Parameter{
				{
					ParameterEntry: model.ParameterEntry{
						Name:  "flexible_param",
						Value: nil,
					},
					Type:        "",
					IsDynamic:   true,
					IsImmutable: false,
				},
			},
			entry:       testutil.NewParameterEntry("flexible_param", map[string]interface{}{"complex": "value"}),
			expectError: false,
			description: "无 schema 定义的参数应接受任何类型的值",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewParameterValidator(tt.constraints)
			err := validator.Validate(tt.entry)

			if tt.expectError {
				assert.NotNil(t, err, tt.description)
				if err != nil {
					assert.Equal(t, tt.errorCode, err.ErrorCode, "error code should match")
				}
			} else {
				assert.Nil(t, err, tt.description)
			}
		})
	}
}

func TestParameterValidator_Validate_MixedParameters(t *testing.T) {
	// 部分参数有完整 schema，部分只在列表中声明
	minVal := float64(1)
	maxVal := float64(1000)
	constraints := []model.Parameter{
		// 有完整 schema 的参数
		testutil.NewParameterConstraint("max_connections").
			WithType(model.ParameterTypeInteger).
			WithRange(&minVal, &maxVal).
			Build(),
		// 只在列表中声明的参数
		{
			ParameterEntry: model.ParameterEntry{
				Name:  "lower_case_table_names",
				Value: nil,
			},
			Type:        "", // 无 schema
			IsDynamic:   false,
			IsImmutable: false,
		},
		{
			ParameterEntry: model.ParameterEntry{
				Name:  "datadir",
				Value: nil,
			},
			Type:        "", // 无 schema
			IsDynamic:   false,
			IsImmutable: true, // 且不可变
		},
	}

	validator := NewParameterValidator(constraints)

	tests := []struct {
		name        string
		entry       model.ParameterEntry
		expectError bool
		errorCode   ParameterErrCode
	}{
		{
			name:        "valid_parameter_with_schema",
			entry:       testutil.NewParameterEntry("max_connections", 500),
			expectError: false,
		},
		{
			name:        "invalid_parameter_with_schema_out_of_range",
			entry:       testutil.NewParameterEntry("max_connections", 2000),
			expectError: true,
			errorCode:   ParamOutOfRange,
		},
		{
			name:        "valid_parameter_without_schema",
			entry:       testutil.NewParameterEntry("lower_case_table_names", "0"),
			expectError: false,
		},
		{
			name:        "immutable_parameter_without_schema",
			entry:       testutil.NewParameterEntry("datadir", "/new/path"),
			expectError: true,
			errorCode:   ParamImmutable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.Validate(tt.entry)

			if tt.expectError {
				assert.NotNil(t, err)
				if err != nil {
					assert.Equal(t, tt.errorCode, err.ErrorCode)
				}
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
