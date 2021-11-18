package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	utils "xqledger/rdbreader/utils"
	pb "xqledger/rdbreader/protobuf"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

const id = "111111111111111111111111"
const dbName = "TestRepository"
const colName = "main"
const name = "John"
const surname = "Wayne"
const age = 55

func insertRecord(client *mongo.Client, ctx context.Context, dbName string, colName string, _id string, recordAsMap map[string]interface{}) (string, error) {
	methodMsg := "insertRecord"
	cleanDbName := strings.ReplaceAll(dbName, ".", "")
	if !(len(colName) > 0){
		colName = "main"
	}
	col := client.Database(cleanDbName).Collection(colName)
	
	if len(_id) > 0 { 
		oid, _ := primitive.ObjectIDFromHex(_id)
		recordAsMap["_id"] = oid
	} else {
		err := errors.New("ID not provided")
		return "", err
	}
	col.InsertOne(ctx, recordAsMap)
	utils.PrintLogInfo(componentMessage, methodMsg, fmt.Sprintf("New record inserted successfully with ID '%s' - Database '%s' - Collection '%s'", id, dbName, colName))
	
	return id, nil
}

func deleteRecord(client *mongo.Client, ctx context.Context, dbName string, colName string, _id string) (string, error) {
	methodMsg := "deleteRecord"
	cleanDbName := strings.ReplaceAll(dbName, ".", "")
	if !(len(colName) > 0){
		colName = "main"
	}
	col := client.Database(cleanDbName).Collection(colName)
	
	if !(len(_id) > 0) { 
		err := errors.New("ID not provided")
		return "", err
	}

	_, insertErr := col.DeleteOne(ctx, _id)
	if insertErr != nil {
		utils.PrintLogError(insertErr, componentMessage, methodMsg, "Error deleting record in RDB")
		return "", insertErr
	}
	utils.PrintLogInfo(componentMessage, methodMsg, fmt.Sprintf("New record deleted successfully with ID '%s' - Database '%s' - Collection '%s'", _id, dbName, colName))
	
	return id, nil
}

func getCriteriaSet() []*pb.Criteria{

	var criteria []*pb.Criteria
	criteria1 := &pb.Criteria{
		BooleanOperator:  "AND",
		Field: "name",
		Is: "EQUAL",
		Value: name,
	}

	criteria = append(criteria, criteria1)
	criteria2 := &pb.Criteria{
		BooleanOperator:  "AND",
		Field: "surname",
		Is: "EQUAL",
		Value: surname,
	}

	criteria = append(criteria, criteria2)

	return criteria
}

func TestRDBClient(t *testing.T) {
	record := make(map[string]interface{})
    record["_id"] = id
	record["name"] = name
	record["surname"] = surname
	record["age"] = age

	client, clientErr := getRDBClient()
	if clientErr != nil {
		log.Fatal(clientErr.Error())
		return
	}
	insertRecord(client, context.TODO(), dbName, colName, id, record)
	Convey("Check RDB query", t, func() {
		query := &pb.RDBQuery{
			DatabaseName:  dbName,
			CollectionName: colName,
		}
		var criteria = getCriteriaSet()
		query.Query = criteria
		result, err := RunQuery("TestRepository", "main", query)
		So(err, ShouldBeNil)
		So(len(result), ShouldBeGreaterThan, 0)
	})

	Convey("Check RDB get whole collection", t, func() {
		result, err := GetAllRecordsFromCollection("TestRepository", "main")
		So(err, ShouldBeNil)
		So(len(result), ShouldBeGreaterThan, 0)
	})

	deleteRecord(client, context.TODO(), dbName, colName, id)
}