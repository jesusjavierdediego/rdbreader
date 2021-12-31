package mongodb

import (
	"context"
	"fmt"
	"time"
	utils "xqledger/rdbreader/utils"
	configuration "xqledger/rdbreader/configuration"
	pb "xqledger/rdbreader/protobuf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"go.mongodb.org/mongo-driver/bson/primitive"
	bq "github.com/samtech09/bsonquery"
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

func criteriaToBsonQuery(criteria []*pb.Criteria) (doc bson.M, err error) {
	var filter = bq.Builder()
	if len(criteria) > 0 {
		// filter := bq.Builder().And(bq.C().EQ("name", "test2"), bq.C().GT("age", 29)).Build()
		//var filter = bq.Builder()
		for _, c := range criteria {
			switch c.BooleanOperator {
			case "AND":
				filter = filter.And(bq.C().EQ(c.Field, c.Value))
			case "OR":
				filter = filter.Or(bq.C().EQ(c.Field, c.Value))
			default:
				filter = filter.And(bq.C().EQ(c.Field, c.Value))
			}
		}
	} 
	return filter.Build(), nil
}

/*
The result is returned in the shape of an array of maps (key: string, value: any type)
*/
func RunQuery(dbName string, colName string, query *pb.RDBQuery) ([]map[string]interface{}, error) {
	methodMsg := "RunQuery"
	rdbClient, err := getRDBClient()
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error getting MongoDB client")
		return nil, err
	}

	bsonQuery, criteriaConversionErr := criteriaToBsonQuery(query.Query)
	if criteriaConversionErr != nil {
		utils.PrintLogError(criteriaConversionErr, componentMessage, methodMsg, "Creating query from criteria - error")
		return nil, criteriaConversionErr
	}

	col := rdbClient.Database(dbName).Collection(colName)
	cursor, err := col.Find(context.TODO(), bsonQuery)
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Finding  documents from query - error")
		
		return nil, err
	}
	defer cursor.Close(ctx)
	var resultSet []map[string]interface{}
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMsg, "Reading cursor decoding error")
		} else {
			var r = make(map[string]interface{})
			for k, v := range result {
				r[k] = v
			}
			resultSet = append(resultSet, r)
		}
	}
	defer cursor.Close(ctx)
	utils.PrintLogInfo(componentMessage, methodMsg, fmt.Sprintf("Records in collection %s, database %s obtained OK", colName, dbName))
	return resultSet, nil
}

func GetAllRecordsFromCollection(dbName string, colName string) ([]map[string]interface{}, error) {
	methodMsg := "GetAllRecordsFromCollection"
	rdbClient, err := getRDBClient()
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error getting MongoDB client")
		return nil, err
	}
	col := rdbClient.Database(dbName).Collection(colName)
	cursor, err := col.Find(context.TODO(), bson.D{})
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Finding all documents from collection - error")
		return nil, err
	}
	defer cursor.Close(ctx)
	var resultSet []map[string]interface{}

	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMsg, "Reading cursor decoding error")
		} else {
			var r = make(map[string]interface{})
			for k, v := range result {
				r[k] = v
			}
			resultSet = append(resultSet, r)
		}
	}
	defer cursor.Close(ctx)
	utils.PrintLogInfo(componentMessage, methodMsg, fmt.Sprintf("Records in collection %s, database %s obtained OK", colName, dbName))
	return resultSet, nil
}

func GetNumberOfRecordsFromCollection(dbName string, colName string) (int64, error) {
	methodMsg := "GetAllRecordsFromCollection"
	rdbClient, err := getRDBClient()
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error getting MongoDB client")
		return 0, err
	}
	col := rdbClient.Database(dbName).Collection(colName)
	count, err := col.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMsg, "Error getting count records from coll")
		return 0, err
	}
	return count, nil
}