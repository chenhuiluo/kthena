/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cert

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"k8s.io/klog/v2"
)

// =============================================================================
// webhook 自签证书生成 — 给 webhook server 生成 TLS 证书
// =============================================================================
// 控制面启动时若没有现成证书, 调本包自签一张(RSA 2048, 10 年有效):
//   生成 CA → 用 CA 签服务端证书(SAN 含 dnsNames, 即 <service>.<ns>.svc 等)。
// 生成后由 secret.go 写入 Secret, 并把 CA bundle 更新到 Validating/MutatingWebhookConfiguration。
// =============================================================================

const (
	// RSAKeySize 证书 RSA 密钥位数
	RSAKeySize = 2048
	// CertValidityYears 证书有效年数
	CertValidityYears = 10
)

// CertBundle 证书包: 服务端证书 + 私钥 + CA 证书(均 PEM 编码)
type CertBundle struct {
	// CertPEM PEM 编码的服务端证书
	CertPEM []byte
	// KeyPEM PEM 编码的私钥
	KeyPEM []byte
	// CAPEM PEM 编码的 CA 证书(需写入 webhook config 的 caBundle)
	CAPEM []byte
}

// GenerateSelfSignedCertificate 为 webhook server 生成自签证书: 生成 CA → 用 CA 签服务端证书(SAN=dnsNames)。
func GenerateSelfSignedCertificate(dnsNames []string) (*CertBundle, error) {
	if len(dnsNames) == 0 {
		return nil, fmt.Errorf("dnsNames cannot be empty")
	}

	klog.Infof("Generating self-signed certificate for DNS names: %v", dnsNames)

	// Generate CA certificate and key
	caKey, err := rsa.GenerateKey(rand.Reader, RSAKeySize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA key: %w", err)
	}

	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA serial number: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			CommonName:   "kthena-webhook-ca",
			Organization: []string{"Volcano"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(CertValidityYears, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}

	// Generate server certificate and key
	serverKey, err := rsa.GenerateKey(rand.Reader, RSAKeySize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate server key: %w", err)
	}

	serverSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("failed to generate server serial number: %w", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject: pkix.Name{
			CommonName:   dnsNames[0],
			Organization: []string{"Volcano"},
		},
		DNSNames:    dnsNames,
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(CertValidityYears, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create server certificate: %w", err)
	}

	// Encode CA certificate to PEM
	caCertPEM := new(bytes.Buffer)
	if err := pem.Encode(caCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	}); err != nil {
		return nil, fmt.Errorf("failed to encode CA certificate: %w", err)
	}

	// Encode server certificate to PEM
	serverCertPEM := new(bytes.Buffer)
	if err := pem.Encode(serverCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCertDER,
	}); err != nil {
		return nil, fmt.Errorf("failed to encode server certificate: %w", err)
	}

	// Encode server key to PEM
	serverKeyPEM := new(bytes.Buffer)
	if err := pem.Encode(serverKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverKey),
	}); err != nil {
		return nil, fmt.Errorf("failed to encode server key: %w", err)
	}

	klog.Info("Successfully generated self-signed certificate")

	return &CertBundle{
		CertPEM: serverCertPEM.Bytes(),
		KeyPEM:  serverKeyPEM.Bytes(),
		CAPEM:   caCertPEM.Bytes(),
	}, nil
}
