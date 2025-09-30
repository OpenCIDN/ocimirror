module github.com/OpenCIDN/OpenCIDN

go 1.24

require (
	github.com/aws/aws-sdk-go v1.55.7
	github.com/docker/distribution v2.8.3+incompatible
	github.com/emicklei/go-restful-openapi/v2 v2.11.0
	github.com/emicklei/go-restful/v3 v3.12.1
	github.com/go-openapi/spec v0.20.9
	github.com/go-sql-driver/mysql v1.9.0
	github.com/google/go-containerregistry v0.20.3
	github.com/gorilla/handlers v1.5.2
	github.com/spf13/cobra v1.9.1
	github.com/wzshiming/cmux v0.4.2
	github.com/wzshiming/hostmatcher v0.0.3
	github.com/wzshiming/httpseek v0.5.0
	github.com/wzshiming/imc v0.0.0-20250106051804-1cb884b5184a
	github.com/wzshiming/sss v0.2.1
	golang.org/x/crypto v0.36.0
	golang.org/x/time v0.10.0
)

replace github.com/docker/distribution => github.com/distribution/distribution v2.8.3+incompatible

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/docker/cli v27.5.0+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.8.2 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	github.com/wzshiming/trie v0.3.1 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
