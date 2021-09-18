package mongodb

import (
	"context"
	"fmt"
	"time"
	utils "xqledger/rdbreader/utils"
	configuration "xqledger/rdbreader/configuration"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"go.mongodb.org/mongo-driver/bson/primitive"
)

const componentMessage = "MongoDB Client"

var config = configuration.GlobalConfiguration
var client *mongo.Client = nil
var ctx context.Context


func getRDBClient() (*mongo.Client, error) {
	methodMsg := "getRDBClient"
	if client != nil {
		utils.PrintLogInfo(componentMessage, methodMsg, "Existing MongoDB Client obtained OK")
		return client, nil
	}
	uri := fmt.Sprintf(
		"mongodb://%s:%s@%s:%d/admin?authSource=admin",
		config.Rdb.Username,
		config.Rdb.Password,
		config.Rdb.Host,
		27017,
	)
	if ctx == nil {
		c, _ := context.WithTimeout(context.Background(), time.Duration(config.Rdb.Timeout) * time.Second)
		ctx = c
	}
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions = clientOptions.SetMaxPoolSize(uint64(config.Rdb.Poolsize))
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error connecting to MongoDB")
		return nil, err
	}
	utils.PrintLogInfo(componentMessage, methodMsg, "New MongoDB Client obtained OK")
	return client, nil
}

/*
The result is returned in the shape of an array of maps (key: string, value: any type)
*/
func RunQuery(dbName string, colName string, query string) ([]map[string]interface{}, error) {
	methodMsg := "RunQuery"
	rdbClient, err := getRDBClient()
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error getting MongoDB client")
		return nil, err
	}
	col := rdbClient.Database(dbName).Collection(colName)
	cursor, err := col.Find(context.TODO(), bson.D{})
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Finding all documents error")
		defer cursor.Close(ctx)
	}
	var resultSet []map[string]interface{}

	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMsg, "Reading cursor decoding error")
		} else {
			//mongoId := result["_id"]
			//mongoIdAsStr := mongoId.(primitive.ObjectID).Hex()
			var r = make(map[string]interface{})
			for k, v := range result {
				// if k == "_id" {
				// 	r[k] = mongoIdAsStr
				// } else {
				// 	r[k] = v
				// }
				r[k] = v
			}
			resultSet = append(resultSet, r)
		}
	}
	defer cursor.Close(ctx)
	utils.PrintLogInfo(componentMessage, methodMsg, fmt.Sprintf("Records in collection %s, database %s obtained OK", colName, dbName))
	return resultSet, nil
}