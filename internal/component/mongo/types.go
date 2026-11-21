package mongo

import (
	"github.com/dobyte/due/v2/component"
	"go.mongodb.org/mongo-driver/mongo"
)

// Mongo is the MongoDB component.
type Mongo struct {
	component.Base
	client   *mongo.Client
	database *mongo.Database
}
