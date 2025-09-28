package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/furutachiKurea/block-mechanica/internal/testutil"

	opsv1alpha1 "github.com/apecloud/kubeblocks/apis/operations/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCleanupClusterOpsRequests(t *testing.T) {
	ctx := context.Background()
	clusterName := "test-cluster"
	cluster := testutil.NewMySQLCluster(clusterName, testutil.TestNamespace).
		WithServiceID(testutil.TestServiceID).
		Build()

	tests := []struct {
		name        string
		clientSetup func() client.Client
		setup       func(client.Client) error
		expectErr   bool
		errContains string
		verify      func(*testing.T, client.Client)
	}{
		{
			name: "non_final_list_error",
			clientSetup: func() client.Client {
				return testutil.NewErrorClientBuilder().
					WithListError(errors.New("list failed")).
					Build()
			},
			expectErr:   true,
			errContains: "get existing opsrequests",
		},
		{
			name: "blocking_cleanup_error",
			clientSetup: func() client.Client {
				return testutil.NewErrorClientBuilder().
					WithPatchError(errors.New("patch failed")).
					Build()
			},
			setup: func(c client.Client) error {
				blockingCancel := testutil.NewOpsRequestBuilder("cancel-block", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.HorizontalScalingType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build()
				blockingExpire := testutil.NewOpsRequestBuilder("expire-block", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.RestartType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsPendingPhase).
					Build()
				timeout := int32(300)
				blockingExpire.Spec.TimeoutSeconds = &timeout
				return testutil.CreateObjects(ctx, c, []client.Object{blockingCancel, blockingExpire})
			},
			expectErr:   true,
			errContains: "cleanup blocking ops",
		},
		{
			name: "all_ops_list_error",
			clientSetup: func() client.Client {
				return testutil.NewErrorClientBuilder().
					WithListError(errors.New("list all failed")).
					Build()
			},
			expectErr:   true,
			errContains: "get existing opsrequests",
		},
		{
			name: "no_ops_present",
			clientSetup: func() client.Client {
				return testutil.NewFakeClientWithIndexes()
			},
			verify: func(t *testing.T, c client.Client) {
				var list opsv1alpha1.OpsRequestList
				err := c.List(ctx, &list, client.InNamespace(testutil.TestNamespace))
				require.NoError(t, err)
				assert.Len(t, list.Items, 0)
			},
		},
		{
			name: "cleanup_and_delete_all",
			clientSetup: func() client.Client {
				return testutil.NewFakeClientWithIndexes()
			},
			setup: func(c client.Client) error {
				blockingCancel := testutil.NewOpsRequestBuilder("cancel-block", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.HorizontalScalingType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build()
				blockingExpire := testutil.NewOpsRequestBuilder("expire-block", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.RestartType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build()
				timeout := int32(600)
				blockingExpire.Spec.TimeoutSeconds = &timeout
				finalOps := testutil.NewOpsRequestBuilder("final", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.BackupType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsSucceedPhase).
					Build()
				return testutil.CreateObjects(ctx, c, []client.Object{blockingCancel, blockingExpire, finalOps})
			},
			verify: func(t *testing.T, c client.Client) {
				var list opsv1alpha1.OpsRequestList
				err := c.List(ctx, &list, client.InNamespace(testutil.TestNamespace))
				require.NoError(t, err)
				assert.Len(t, list.Items, 0)
			},
		},
		{
			name: "only_final_ops",
			clientSetup: func() client.Client {
				return testutil.NewFakeClientWithIndexes()
			},
			setup: func(c client.Client) error {
				finalOps := []client.Object{
					testutil.NewOpsRequestBuilder("succeeded", testutil.TestNamespace).
						WithClusterName(clusterName).
						WithType(opsv1alpha1.BackupType).
						WithInstanceLabel(clusterName).
						WithPhase(opsv1alpha1.OpsSucceedPhase).
						Build(),
					testutil.NewOpsRequestBuilder("failed", testutil.TestNamespace).
						WithClusterName(clusterName).
						WithType(opsv1alpha1.RestoreType).
						WithInstanceLabel(clusterName).
						WithPhase(opsv1alpha1.OpsFailedPhase).
						Build(),
				}
				return testutil.CreateObjects(ctx, c, finalOps)
			},
			verify: func(t *testing.T, c client.Client) {
				var list opsv1alpha1.OpsRequestList
				err := c.List(ctx, &list, client.InNamespace(testutil.TestNamespace))
				require.NoError(t, err)
				assert.Len(t, list.Items, 0)
			},
		},
		{
			name: "delete_error",
			clientSetup: func() client.Client {
				return testutil.NewErrorClientBuilder().
					WithDeleteError(errors.New("delete failed")).
					Build()
			},
			setup: func(c client.Client) error {
				op := testutil.NewOpsRequestBuilder("delete-me", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.HorizontalScalingType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build()
				return testutil.CreateObjects(ctx, c, []client.Object{op})
			},
			expectErr:   true,
			errContains: "delete all ops",
		},
		{
			name: "ignore_not_found",
			clientSetup: func() client.Client {
				return testutil.NewFakeClientWithIndexes()
			},
			setup: func(c client.Client) error {
				// 不创建任何对象，模拟对象已被其他进程删除的场景
				return nil
			},
			verify: func(t *testing.T, c client.Client) {
				// 验证没有创建任何OpsRequest对象
				var list opsv1alpha1.OpsRequestList
				err := c.List(ctx, &list, client.InNamespace(testutil.TestNamespace))
				require.NoError(t, err)
				assert.Len(t, list.Items, 0)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := tt.clientSetup()
			svc := &Service{client: k8sClient}

			if tt.setup != nil {
				require.NoError(t, tt.setup(k8sClient))
			}

			err := svc.cleanupClusterOpsRequests(ctx, cluster.DeepCopy())

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, k8sClient)
			}
		})
	}
}

func TestDeleteAllOpsRequestsConcurrently(t *testing.T) {
	ctx := context.Background()
	clusterName := "test-cluster"

	tests := []struct {
		name        string
		clientSetup func() client.Client
		setup       func(client.Client) error
		ops         []opsv1alpha1.OpsRequest
		expectErr   bool
		errContains string
		verify      func(*testing.T, client.Client)
	}{
		{
			name:        "empty_list",
			clientSetup: func() client.Client { return testutil.NewFakeClientWithIndexes() },
			ops:         nil,
		},
		{
			name:        "delete_success",
			clientSetup: func() client.Client { return testutil.NewFakeClientWithIndexes() },
			setup: func(c client.Client) error {
				ops := []client.Object{
					testutil.NewOpsRequestBuilder("success-1", testutil.TestNamespace).
						WithClusterName(clusterName).
						WithType(opsv1alpha1.HorizontalScalingType).
						WithInstanceLabel(clusterName).
						WithPhase(opsv1alpha1.OpsSucceedPhase).
						Build(),
					testutil.NewOpsRequestBuilder("success-2", testutil.TestNamespace).
						WithClusterName(clusterName).
						WithType(opsv1alpha1.VerticalScalingType).
						WithInstanceLabel(clusterName).
						WithPhase(opsv1alpha1.OpsCancelledPhase).
						WithCancel().
						Build(),
				}
				return testutil.CreateObjects(ctx, c, ops)
			},
			ops: []opsv1alpha1.OpsRequest{
				*testutil.NewOpsRequestBuilder("success-1", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.HorizontalScalingType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsSucceedPhase).
					Build(),
				*testutil.NewOpsRequestBuilder("success-2", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.VerticalScalingType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsCancelledPhase).
					WithCancel().
					Build(),
			},
			verify: func(t *testing.T, c client.Client) {
				for _, name := range []string{"success-1", "success-2"} {
					op := &opsv1alpha1.OpsRequest{}
					err := c.Get(ctx, types.NamespacedName{Namespace: testutil.TestNamespace, Name: name}, op)
					assert.Error(t, err)
					assert.True(t, apierrors.IsNotFound(err))
				}
			},
		},
		{
			name: "delete_not_found",
			clientSetup: func() client.Client {
				return testutil.NewFakeClientWithIndexes()
			},
			setup: func(c client.Client) error {
				// 不创建任何对象，模拟要删除的对象已被其他进程删除
				return nil
			},
			ops: []opsv1alpha1.OpsRequest{
				*testutil.NewOpsRequestBuilder("gone", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.RestoreType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build(),
			},
		},
		{
			name: "delete_error",
			clientSetup: func() client.Client {
				return testutil.NewErrorClientBuilder().
					WithDeleteError(errors.New("delete failed")).
					Build()
			},
			setup: func(c client.Client) error {
				ops := []client.Object{
					testutil.NewOpsRequestBuilder("bad", testutil.TestNamespace).
						WithClusterName(clusterName).
						WithType(opsv1alpha1.BackupType).
						WithInstanceLabel(clusterName).
						WithPhase(opsv1alpha1.OpsRunningPhase).
						Build(),
				}
				return testutil.CreateObjects(ctx, c, ops)
			},
			ops: []opsv1alpha1.OpsRequest{
				*testutil.NewOpsRequestBuilder("bad", testutil.TestNamespace).
					WithClusterName(clusterName).
					WithType(opsv1alpha1.BackupType).
					WithInstanceLabel(clusterName).
					WithPhase(opsv1alpha1.OpsRunningPhase).
					Build(),
			},
			expectErr:   true,
			errContains: "failed to delete opsrequest bad",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := tt.clientSetup()
			svc := &Service{client: k8sClient}

			if tt.setup != nil {
				require.NoError(t, tt.setup(k8sClient))
			}

			err := svc.deleteAllOpsRequestsConcurrently(ctx, tt.ops)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, k8sClient)
			}
		})
	}
}
