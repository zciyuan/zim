package pushoffline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPProviderSendsSignedBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ZIM-Signature") == "" {
			t.Error("missing signature")
		}
		var body struct {
			Notifications []Notification `json:"notifications"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Notifications) != 1 {
			t.Fatalf("notifications = %d", len(body.Notifications))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []Result{{Token: "a", OK: true}},
		})
	}))
	defer server.Close()

	provider := NewHTTPProvider("http", server.URL, "secret", 0)
	results, err := provider.Send(context.Background(), []Notification{{Token: "a", Title: "t"}})
	if err != nil || len(results) != 1 || !results[0].OK {
		t.Fatalf("results = %+v, err = %v", results, err)
	}
}

func TestRouterUsesExplicitAndPlatformProviders(t *testing.T) {
	httpProvider := &fakeProvider{results: []Result{{Token: "a", OK: true}}}
	defaultProvider := &fakeProvider{results: []Result{{Token: "b", OK: true}}}
	router := NewRouter("dummy", map[string]Provider{
		"http": httpProvider, "dummy": defaultProvider,
	}, map[int16]string{2: "http"})

	results, err := router.Send(context.Background(), []Notification{
		{Token: "a", Provider: "http"},
		{Token: "b", Platform: 3},
	})
	if err != nil || len(results) != 2 || !results[0].OK || !results[1].OK {
		t.Fatalf("results = %+v, err = %v", results, err)
	}
	if len(httpProvider.sent) != 1 || len(defaultProvider.sent) != 1 {
		t.Fatalf("http provider sent = %d", len(httpProvider.sent))
	}
}
