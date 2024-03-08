module go.temporal.io/server

go 1.21

toolchain go1.22.0

require (
	cloud.google.com/go/storage v1.30.1
	github.com/DataDog/dd-source v0.0.0-20240308131425-aa18a9120571
	github.com/aws/aws-sdk-go v1.45.28
	github.com/blang/semver/v4 v4.0.0
	github.com/brianvoe/gofakeit/v6 v6.24.0
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v1.18.1
	github.com/fatih/color v1.16.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gocql/gocql v1.5.2
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/golang/mock v1.7.0-rc.1
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.4.0
	github.com/iancoleman/strcase v0.2.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/jonboulle/clockwork v0.4.0
	github.com/lib/pq v1.10.9
	github.com/olekukonko/tablewriter v0.0.5
	github.com/olivere/elastic/v7 v7.0.32
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/client_golang v1.15.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.8.4
	github.com/temporalio/ringpop-go v0.0.0-20220818230611-30bf23b490b2
	github.com/temporalio/tchannel-go v1.22.1-0.20220818200552-1be8d8cffa5b
	github.com/temporalio/tctl-kit v0.0.0-20230213052353-2342ea1e7d14
	github.com/uber-go/tally/v4 v4.1.6
	github.com/urfave/cli v1.22.12
	github.com/urfave/cli/v2 v2.25.7
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.42.0
	go.opentelemetry.io/otel v1.20.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.16.0
	go.opentelemetry.io/otel/exporters/prometheus v0.39.0
	go.opentelemetry.io/otel/metric v1.20.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/sdk/metric v0.39.0
	go.temporal.io/api v1.23.0
	go.temporal.io/sdk v1.23.1
	go.temporal.io/version v0.3.0
	go.uber.org/atomic v1.11.0
	go.uber.org/automaxprocs v1.5.2
	go.uber.org/fx v1.19.1
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/oauth2 v0.14.0
	golang.org/x/sync v0.6.0
	golang.org/x/time v0.3.0
	google.golang.org/api v0.149.0
	google.golang.org/grpc v1.61.0
	google.golang.org/grpc/examples v0.0.0-20230216223317-abff344ead8f
	gopkg.in/square/go-jose.v2 v2.6.0
	gopkg.in/validator.v2 v2.0.1
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.28.0
)

require (
	github.com/DataDog/apis v1.28.6 // indirect
	github.com/DataDog/appsec-internal-go v1.4.1 // indirect
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.48.0 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.48.1 // indirect
	github.com/DataDog/datadog-go/v5 v5.5.0 // indirect
	github.com/DataDog/go-libddwaf/v2 v2.3.1 // indirect
	github.com/DataDog/go-service-authn v1.6.3 // indirect
	github.com/DataDog/go-tuf v1.0.2-0.5.2 // indirect
	github.com/DataDog/hyperloglog v0.0.0-20220804205443-1806d9b66146 // indirect
	github.com/DataDog/mmh3 v0.0.0-20210722141835-012dc69a9e49 // indirect
	github.com/DataDog/sketches-go v1.4.2 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/VividCortex/multitick v1.0.0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20231109132714-523115ebc101 // indirect
	github.com/ebitengine/purego v0.6.0-alpha.5 // indirect
	github.com/envoyproxy/go-control-plane v0.11.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.2 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jamiealquiza/bicache/v2 v2.4.0 // indirect
	github.com/jamiealquiza/fnv v1.0.0 // indirect
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible // indirect
	github.com/miekg/dns v1.1.55 // indirect
	github.com/outcaste-io/ristretto v0.2.3 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.8.0 // indirect
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/vaughan0/go-ini v0.0.0-20130923145212-a98ad7ee00ec // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.uber.org/mock v0.3.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231106174013-bbf56f31fb17 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231106174013-bbf56f31fb17 // indirect
	gopkg.in/DataDog/dd-trace-go.v1 v1.61.0-rc.1 // indirect
)

require (
	cloud.google.com/go v0.110.10 // indirect
	cloud.google.com/go/compute v1.23.3 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.5 // indirect
	github.com/apache/thrift v0.18.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.3 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.2 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.4.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.6 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	github.com/twmb/murmur3 v1.1.6 // indirect
	github.com/uber-common/bark v1.3.0 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.16.0 // indirect
	go.opentelemetry.io/otel/trace v1.20.0
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/dig v1.16.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/mod v0.14.0 // indirect
	golang.org/x/net v0.20.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.16.1 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20231106174013-bbf56f31fb17 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	lukechampine.com/uint128 v1.3.0 // indirect
	modernc.org/cc/v3 v3.41.0 // indirect
	modernc.org/ccgo/v3 v3.16.15 // indirect
	modernc.org/libc v1.32.0 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.7.2 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.2.0 // indirect
	modernc.org/token v1.1.0 // indirect
)

// replace required because of dd-source import, copy/pasted from dd-source
replace (
	// Cannot upgrade otel dependencies because an incompatibility on `go.temporal.io/server`
	go.opentelemetry.io/otel => go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/metric => go.opentelemetry.io/otel/metric v1.16.0
	go.opentelemetry.io/otel/trace => go.opentelemetry.io/otel/trace v1.16.0
)
