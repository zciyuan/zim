package changelog

import "go.mongodb.org/mongo-driver/mongo"

// DAO owns the per-entity change-log collections.
type DAO struct {
	db *mongo.Database
}
