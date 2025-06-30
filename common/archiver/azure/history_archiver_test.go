package azure

import (
	"context"

	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/azure/connector"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"

	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testNamespaceID               = "test-namespace-id"
	testNamespace                 = "test-namespace"
	testWorkflowID                = "test-workflow-id"
	testRunID                     = "test-run-id"
	testNextEventID               = 1800
	testCloseFailoverVersion      = 100
	testPageSize                  = 100
	exampleOldHistoryRecord       = `[{"events":[{"eventId":1,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}}]}]`
	exampleNewHistoryRecord       = `[{"events":[{"eventId":1,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}}]}]`
	twoEventsExampleHistoryRecord = `[{"events":[{"eventId":1,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}},{"eventId":2,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}}]}]`
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type historyArchiverSuite struct {
	*require.Assertions
	suite.Suite

	controller *gomock.Controller

	logger           log.Logger
	metricsHandler   metrics.Handler
	executionManager persistence.ExecutionManager
	testArchivalURI  archiver.URI
}

func (h *historyArchiverSuite) SetupTest() {
	h.Assertions = require.New(h.T())
	h.controller = gomock.NewController(h.T())
	h.logger = log.NewNoopLogger()
	h.metricsHandler = metrics.NoopMetricsHandler
	// Azure Storage URI scheme is "as"
	h.testArchivalURI, _ = archiver.NewURI("as://my-bucket-cad/temporal_archival/development")
}

func (h *historyArchiverSuite) TearDownTest() {
	h.controller.Finish()
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func (h *historyArchiverSuite) TestValidateURI() {
	ctx := context.Background()
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrURISchemeMismatch,
		},
		{
			URI:         "as:my-bucket-cad/temporal_archival/development",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "as://",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "as://my-bucket-cad",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "as:/my-bucket-cad/temporal_archival/development",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "as://my-bucket-cad/temporal_archival/development",
			expectedErr: nil,
		},
	}

	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, gomock.Any(), "").Return(false, nil)
	historyArchiver := new(historyArchiver)
	historyArchiver.azureStorage = storageWrapper
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		h.NoError(err)
		h.Equal(tc.expectedErr, historyArchiver.ValidateURI(URI))
	}
}

func (h *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	mockStorageClient := connector.NewMockClient(h.controller)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)

	historyArchiver := newHistoryArchiver(h.executionManager, h.logger, h.metricsHandler, historyIterator, mockStorageClient)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	h.NoError(err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_InvalidRequest() {
	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)

	historyArchiver := newHistoryArchiver(h.executionManager, h.logger, h.metricsHandler, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           "", // Invalid empty WorkflowID
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}

	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Success() {
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 2,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
			},
		},
	}
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	historyArchiver := h.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}

	err := historyArchiver.Archive(context.Background(), h.testArchivalURI, request)
	h.NoError(err)
}

func (h *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	ctx := context.Background()
	mockStorageClient := connector.NewMockClient(h.controller)
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.executionManager, h.logger, h.metricsHandler, historyIterator, mockStorageClient)

	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    100,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.Nil(response)
	h.Error(err)
}

func (h *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Query(ctx, h.testArchivalURI, gomock.Any()).Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleNewHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.executionManager, h.logger, h.metricsHandler, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}

	response, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_NoHistory() {
	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Query(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{}, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.executionManager, h.logger, h.metricsHandler, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    2,
	}

	_, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
	h.Assert().IsType(&serviceerror.NotFound{}, err)
}

func (h *historyArchiverSuite) newTestHistoryArchiver(historyIterator archiver.HistoryIterator) *historyArchiver {
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), gomock.Any(), "").Return(true, nil).AnyTimes()  // ValidateURI calls
	storageWrapper.EXPECT().Exist(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()  // File existence checks
	storageWrapper.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return &historyArchiver{
		executionManager: h.executionManager,
		logger:           h.logger,
		metricsHandler:   h.metricsHandler,
		azureStorage:     storageWrapper,
		historyIterator:  historyIterator,
	}
}
