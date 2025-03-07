// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute (interfaces: StepExecutor)
//
// Generated by this command:
//
//	mockgen -package execute github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute StepExecutor
//
// Package execute is a generated GoMock package.
package execute

import (
	context "context"
	reflect "reflect"

	proto "github.com/pingcap/tidb/pkg/disttask/framework/proto"
	gomock "go.uber.org/mock/gomock"
)

// MockStepExecutor is a mock of StepExecutor interface.
type MockStepExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockStepExecutorMockRecorder
}

// MockStepExecutorMockRecorder is the mock recorder for MockStepExecutor.
type MockStepExecutorMockRecorder struct {
	mock *MockStepExecutor
}

// NewMockStepExecutor creates a new mock instance.
func NewMockStepExecutor(ctrl *gomock.Controller) *MockStepExecutor {
	mock := &MockStepExecutor{ctrl: ctrl}
	mock.recorder = &MockStepExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStepExecutor) EXPECT() *MockStepExecutorMockRecorder {
	return m.recorder
}

// Cleanup mocks base method.
func (m *MockStepExecutor) Cleanup(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cleanup", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Cleanup indicates an expected call of Cleanup.
func (mr *MockStepExecutorMockRecorder) Cleanup(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cleanup", reflect.TypeOf((*MockStepExecutor)(nil).Cleanup), arg0)
}

// Init mocks base method.
func (m *MockStepExecutor) Init(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockStepExecutorMockRecorder) Init(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockStepExecutor)(nil).Init), arg0)
}

// OnFinished mocks base method.
func (m *MockStepExecutor) OnFinished(arg0 context.Context, arg1 *proto.Subtask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnFinished", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// OnFinished indicates an expected call of OnFinished.
func (mr *MockStepExecutorMockRecorder) OnFinished(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnFinished", reflect.TypeOf((*MockStepExecutor)(nil).OnFinished), arg0, arg1)
}

// RunSubtask mocks base method.
func (m *MockStepExecutor) RunSubtask(arg0 context.Context, arg1 *proto.Subtask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunSubtask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RunSubtask indicates an expected call of RunSubtask.
func (mr *MockStepExecutorMockRecorder) RunSubtask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunSubtask", reflect.TypeOf((*MockStepExecutor)(nil).RunSubtask), arg0, arg1)
}
