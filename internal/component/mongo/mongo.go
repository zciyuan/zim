// Package mongo owns the process-wide MongoDB client used for message
// aggregate documents and version logs.
package mongo

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// New returns the MongoDB component. An empty URI leaves the database nil.
func New() *Mongo {
	uri := etc.Get("etc.zim.mongo.uri").String()
	if uri == "" {
		return &Mongo{}
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("create mongo client failed: %v", err)
	}
	return &Mongo{
		client:   client,
		database: client.Database(etc.Get("etc.zim.mongo.database", "zim").String()),
	}
}

// Name returns the component name.
func (m *Mongo) Name() string {
	return "mongo"
}

// Start verifies connectivity once.
func (m *Mongo) Start() {
	if m.client == nil {
		log.Warn("mongo disabled: etc.zim.mongo.uri is empty")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := m.client.Ping(ctx, nil)
	cancel()
	if err != nil {
		log.Fatalf("mongo ping failed: %v", err)
	}
	log.Info("mongo connected")
}

// Close closes the client.
func (m *Mongo) Close() {
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.client.Disconnect(ctx); err != nil {
			log.Warnf("mongo disconnect failed: %v", err)
		}
	}
}

// Database returns the configured database, or nil when Mongo is disabled.
func (m *Mongo) Database() *mongo.Database {
	return m.database
}
