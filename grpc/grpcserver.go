package grpc

import (
	"fmt"
	"encoding/json"
	"xqledger/rdbreader/utils"
	configuration "xqledger/rdbreader/configuration"
	"xqledger/rdbreader/mongodb"
	pb "xqledger/rdbreader/protobuf"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

const componentMessage = "GRPC Server"

var config = configuration.GlobalConfiguration

type RDBQueryService struct {
	query *pb.RDBQuery
}

func NewRDBQueryService(query *pb.RDBQuery) *RDBQueryService {
	return &RDBQueryService{query: query}
}


func (s *RDBQueryService) GetRDBRecords(ctx context.Context, query *pb.RDBQuery) (*pb.RecordSet, error) {
	methodMessage := "GetRDBRecords"
	var resultSet []map[string]interface{}
	var err error
	if !(len(query.Query) > 0) {
		resultSet, err = mongodb.GetAllRecordsFromCollection(query.DatabaseName, "main")
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMessage, "Error querying RDB")
			return nil, status.New(14, "Error getting RDB collection - Reason: "+err.Error()).Err()
		}
	} else {
		resultSet, err = mongodb.RunQuery(query.DatabaseName, "main", query) 
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMessage, "Error querying RDB")
			return nil, status.New(14, "Error querying RDB - Reason: "+err.Error()).Err()
		}
	}
	var arrayOfRecordsStr []string
	for _, record := range resultSet {
		recordStr, err := json.Marshal(record)
		if err != nil {
			utils.PrintLogError(err, componentMessage, methodMessage, "Response content cannot be marshaled properly")
			return nil, status.New(15, "Response content cannot be marshaled properly - Reason: "+err.Error()).Err()
		}
		arrayOfRecordsStr = append(arrayOfRecordsStr, string(recordStr))
	}
	result := pb.RecordSet{Records: arrayOfRecordsStr}
	return &result, nil
}


func (s *RDBQueryService) GetNumberRecordsFromColl(ctx context.Context, query *pb.RDBQuery) (*pb.RDCColCount, error) {
	methodMessage := "GetNumberRecordsFromColl"
	var result pb.RDCColCount
	dbName := query.DatabaseName
	colName := query.CollectionName
	if !(len(dbName)> 0) || !(len(colName)>0){
		msg := "DB name or collection name are empty"
		err := errors.New(msg)
		utils.PrintLogError(err, componentMessage, methodMessage, msg)
		return nil, status.New(3, "Bad request - Reason: "+err.Error()).Err()
	}
	count, countErr := mongodb.GetNumberOfRecordsFromCollection(query.DatabaseName, query.CollectionName)
	if countErr != nil {
		utils.PrintLogError(countErr, componentMessage, methodMessage, fmt.Sprintf("Error getting number of records of db '%s' collection '%s'", query.DatabaseName, query.CollectionName))
		return nil, status.New(15, "Error getting number of records - Reason: "+countErr.Error()).Err()
	}
	result.Count = count
	return &result, nil
}


// rpc GetRDBRecords(RDBQuery) returns (RecordSet){}
//     rpc GetRDBRecordsFromQuery(RDBQuery) returns (RecordSet){}
//     rpc GetNumberRecordsFromColl(RDBQuery) returns (RDCColCount){}

// func (c *recordQueryServiceClient) GetRDBRecordsFromQuery(ctx context.Context, in *RDBQuery, opts ...grpc.CallOption) (*RecordSet, error) {
// 	out := new(RecordSet)
// 	err := c.cc.Invoke(ctx, "/rdboperatorproto.RecordQueryService/GetRDBRecordsFromQuery", in, out, opts...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return out, nil
// }

// func (c *recordQueryServiceClient) GetNumberRecordsFromColl(ctx context.Context, in *RDBQuery, opts ...grpc.CallOption) (*RDCColCount, error) {
// 	out := new(RDCColCount)
// 	err := c.cc.Invoke(ctx, "/rdboperatorproto.RecordQueryService/GetNumberRecordsFromColl", in, out, opts...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return out, nil
// }