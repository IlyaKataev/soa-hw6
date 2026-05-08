package sr

import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

type noAuthProvider struct{}

func (noAuthProvider) GetAuthenticationHeader() (string, error) {
	return "", nil
}

func (noAuthProvider) GetIdentityPoolID() (string, error) {
	return "", nil
}

func (noAuthProvider) GetLogicalCluster() (string, error) {
	return "", nil
}

func NewConfig(url string) *schemaregistry.Config {
	config := schemaregistry.NewConfig(url)
	config.BearerAuthCredentialsSource = "CUSTOM"
	config.AuthenticationHeaderProvider = noAuthProvider{}
	return config
}
