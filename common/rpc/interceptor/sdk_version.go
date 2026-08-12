package interceptor

import (
	"context"
	"sync"

	"github.com/blang/semver/v4"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/versioninfo"
	"google.golang.org/grpc"
)

type SDKVersionInterceptor struct {
	sync.RWMutex
	sdkInfoSet     map[versioninfo.SDKInfo]struct{}
	versionChecker headers.VersionChecker
	maxSetSize     int
}

const defaultMaxSetSize = 100

// namespaceMinSDKVersions enforces a stricter minimum SDK version than the
// cluster default for namespaces that have opted into requiring a newer SDK
// release (e.g. to pick up a fix their workflows depend on).
var namespaceMinSDKVersions = map[string]semver.Range{
	"v1:calypso-6": semver.MustParseRange(">=1.40.0"),
}

// NewSDKVersionInterceptor creates a new SDKVersionInterceptor with default max set size
func NewSDKVersionInterceptor() *SDKVersionInterceptor {
	return &SDKVersionInterceptor{
		sdkInfoSet:     make(map[versioninfo.SDKInfo]struct{}),
		versionChecker: headers.NewDefaultVersionChecker(),
		maxSetSize:     defaultMaxSetSize,
	}
}

// Intercept a grpc request
func (vi *SDKVersionInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	sdkName, sdkVersion := headers.GetClientNameAndVersion(ctx)
	if sdkName != "" && sdkVersion != "" {
		vi.RecordSDKInfo(sdkName, sdkVersion)
		if err := vi.versionChecker.ClientSupported(ctx); err != nil {
			return nil, err
		}
		if err := checkNamespaceMinSDKVersion(req, sdkName, sdkVersion); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

// checkNamespaceMinSDKVersion enforces namespaceMinSDKVersions for requests
// carrying a namespace, e.g. poll requests from workers.
func checkNamespaceMinSDKVersion(req interface{}, sdkName, sdkVersion string) error {
	if sdkName != headers.ClientNameGoSDK {
		return nil
	}
	nsGetter, ok := req.(NamespaceNameGetter)
	if !ok {
		return nil
	}
	minVersionRange, ok := namespaceMinSDKVersions[nsGetter.GetNamespace()]
	if !ok {
		return nil
	}
	parsedVersion, err := semver.Parse(sdkVersion)
	if err != nil {
		return nil
	}
	if !minVersionRange(parsedVersion) {
		return serviceerror.NewClientVersionNotSupported(sdkVersion, sdkName, ">=1.40.0")
	}
	return nil
}

// RecordSDKInfo records name and version tuple in memory
func (vi *SDKVersionInterceptor) RecordSDKInfo(name, version string) {
	info := versioninfo.SDKInfo{Name: name, Version: version}

	vi.RLock()
	overCap := len(vi.sdkInfoSet) >= vi.maxSetSize
	_, found := vi.sdkInfoSet[info]
	vi.RUnlock()

	if !overCap && !found {
		vi.Lock()
		vi.sdkInfoSet[info] = struct{}{}
		vi.Unlock()
	}
}

// GetAndResetSDKInfo gets all recorded name, version tuples and resets internal records
func (vi *SDKVersionInterceptor) GetAndResetSDKInfo() []versioninfo.SDKInfo {
	vi.Lock()
	currSet := vi.sdkInfoSet
	vi.sdkInfoSet = make(map[versioninfo.SDKInfo]struct{})
	vi.Unlock()

	sdkInfo := make([]versioninfo.SDKInfo, 0, len(currSet))
	for k := range currSet {
		sdkInfo = append(sdkInfo, k)
	}
	return sdkInfo
}
