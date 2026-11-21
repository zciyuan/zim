package pushoffline

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func TestAPNsProviderBuildsToken(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewAPNsProvider("TEAM", "KEY", "com.example.zim",
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	token, err := provider.authToken()
	if err != nil || token == "" {
		t.Fatalf("token = %q err = %v", token, err)
	}
	if provider.Name() != "apns" {
		t.Fatalf("name = %s", provider.Name())
	}
}
