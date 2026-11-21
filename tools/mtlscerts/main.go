// Command mtlscerts generates a development CA, server and client certificate.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	dir := "run/zim-mtls"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ZIM Dev CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	writePEM(dir, "ca.pem", "CERTIFICATE", caDER)
	writePEM(dir, "ca-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey))

	serverKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "zim-admin-mtls"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}
	serverDER, _ := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	writePEM(dir, "server.pem", "CERTIFICATE", serverDER)
	writePEM(dir, "server-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverKey))

	clientKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	client := &x509.Certificate{
		SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "zim-admin-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, _ := x509.CreateCertificate(rand.Reader, client, ca, &clientKey.PublicKey, caKey)
	writePEM(dir, "client.pem", "CERTIFICATE", clientDER)
	writePEM(dir, "client-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey))
}

func writePEM(dir, name, typ string, der []byte) {
	block := pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
	if err := os.WriteFile(filepath.Join(dir, name), block, 0o600); err != nil {
		panic(err)
	}
}
