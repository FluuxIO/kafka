package auth // import "fluux.io/kafka/auth"

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

type PemFiles struct {
	ClientCert string
	ClientKey  string
	CACert     string
}

// NewTLSConfig generates a TLS configuration used to authenticate on server with
// certificates.
// Parameter is a struct containing three pem file path: client cert, client key and CA cert.
func NewTLSConfig(pemFiles PemFiles) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(pemFiles.ClientCert, pemFiles.ClientKey)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(pemFiles.CACert)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}
