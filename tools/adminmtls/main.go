// Command adminmtls terminates mutual TLS in front of the ZIM admin API.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8443", "mTLS listen address")
	target := flag.String("target", "http://127.0.0.1:8080", "upstream admin API")
	caFile := flag.String("ca", "run/zim-mtls/ca.pem", "client CA bundle")
	certFile := flag.String("cert", "run/zim-mtls/server.pem", "server certificate")
	keyFile := flag.String("key", "run/zim-mtls/server-key.pem", "server key")
	flag.Parse()

	caPEM, err := os.ReadFile(*caFile)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		log.Fatal("invalid client CA")
	}
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatal(err)
	}
	upstream, err := url.Parse(*target)
	if err != nil {
		log.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	server := &http.Server{
		Addr: *listen,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		},
		Handler:           proxy,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("admin mTLS proxy on https://%s -> %s", *listen, *target)
	log.Fatal(server.ListenAndServeTLS("", ""))
}
