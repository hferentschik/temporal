package azure

import (
	gomock "go.uber.org/mock/gomock"
	reflect "reflect"
)

type MockQueryParser struct {
	ctrl     *gomock.Controller
	recorder *MockQueryParserMockRecorder
	isgomock struct{}
}

type MockQueryParserMockRecorder struct {
	mock *MockQueryParser
}

func NewMockQueryParser(ctrl *gomock.Controller) *MockQueryParser {
	mock := &MockQueryParser{ctrl: ctrl}
	mock.recorder = &MockQueryParserMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockQueryParser) EXPECT() *MockQueryParserMockRecorder {
	return m.recorder
}

// Parse mocks base method.
func (m *MockQueryParser) Parse(query string) (*parsedQuery, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Parse", query)
	ret0, _ := ret[0].(*parsedQuery)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Parse indicates an expected call of Parse.
func (mr *MockQueryParserMockRecorder) Parse(query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Parse", reflect.TypeOf((*MockQueryParser)(nil).Parse), query)
}
