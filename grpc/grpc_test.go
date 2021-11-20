package grpc

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"time"
	pb "xqledger/rdbreader/protobuf"
	utils "xqledger/rdbreader/utils"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

var rdbreader_address = "localhost:" + strconv.Itoa(config.GrpcServer.Port)
var rdbreader_conn *grpc.ClientConn
var rdbreader_connErr error

const id = "111111111111111111111111"
const dbName = "TestRepository"
const colName = "main"
const name = "John"
const surname = "Wayne"
const age = 55
const query = ""

func getRDBReaderServerConn() (*grpc.ClientConn, error) {
	rdbreader_conn, rdbreader_connErr = grpc.Dial(rdbreader_address, grpc.WithInsecure())
	if rdbreader_connErr != nil {
		log.Fatalf("did not connect: %v", rdbreader_connErr)
		return nil, rdbreader_connErr
	}
	return rdbreader_conn, nil
}

func getCriteriaSet() []*pb.Criteria{

	var criteria []*pb.Criteria
	criteria1 := &pb.Criteria{
		BooleanOperator:  "AND",
		Field: "name",
		Is: "EQUAL",
		Value: "John",
	}

	criteria = append(criteria, criteria1)
	criteria2 := &pb.Criteria{
		BooleanOperator:  "AND",
		Field: "surname",
		Is: "EQUAL",
		Value: "Wayne",
	}

	criteria = append(criteria, criteria2)

	return criteria
}

func GetRecordsFromQuery() (*pb.RecordSet, error) {
	var methodMessage = "GetRecordsFromQuery"
	var emptyResult pb.RecordSet
	rdbreader_conn, rdbreader_connErr = getRDBReaderServerConn()
	if rdbreader_connErr != nil {
		utils.PrintLogError(rdbreader_connErr, componentMessage, methodMessage, "Error in connection")
		return &emptyResult, rdbreader_connErr
	}
	defer rdbreader_conn.Close()
	c := pb.NewRecordQueryServiceClient(rdbreader_conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var q pb.RDBQuery
	q.DatabaseName = dbName
	q.CollectionName = "main"
	q.Query = getCriteriaSet()

	recordSet, err := c.GetRDBRecords(ctx, &q)
	if err != nil {
		utils.PrintLogError(err, componentMessage, methodMessage, "Error in grpc server")
		return &emptyResult, err
	}

	utils.PrintLogInfo(componentMessage, methodMessage, fmt.Sprintf("Number of successfully retrieved records: %d", len(recordSet.Records)))
	return recordSet, nil
}

func TestGetRecordsFromQuery(t *testing.T) {
	Convey("Should RDBRecords", t, func() {
		recordSet, err := GetRecordsFromQuery()
		So(err, ShouldBeNil)
		So(len(recordSet.Records), ShouldNotBeNil)
	})
}
