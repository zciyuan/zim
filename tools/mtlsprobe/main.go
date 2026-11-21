// Command mtlsprobe verifies the local admin mTLS proxy.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	url := flag.String("url", "https://127.0.0.1:8443/admin/ping", "probe URL")
	caFile := flag.String("ca", "run/zim-mtls/ca.pem", "CA bundle")
	certFile := flag.String("cert", "run/zim-mtls/client.pem", "client certificate")
	keyFile := flag.String("key", "run/zim-mtls/client-key.pem", "client key")
	noCert := flag.Bool("no-cert", false, "omit client certificate")
	flag.Parse()

	ca, err := os.ReadFile(*caFile)
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	var certificates []tls.Certificate
	if !*noCert {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			panic(err)
		}
		certificates = []tls.Certificate{cert}
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: certificates,
	}}}
	request, _ := http.NewRequest(http.MethodGet, *url, nil)
	request.Header.Set("X-ZIM-Admin-Token", "dev-admin-token")
	request.Header.Set("X-ZIM-Admin-Source", "zim-admin")
	request.Header.Set("X-ZIM-Admin-Permissions", "*")
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(response.Body)
	fmt.Println(response.StatusCode, string(body))
}
