package grpc

import (
	"encoding/json"
	"xqledger/rdbreader/utils"
	configuration "xqledger/rdbreader/configuration"
	"xqledger/rdbreader/mongodb"
	pb "xqledger/rdbreader/protobuf"

	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

const componentMessage = "GRPC Server"

var config = configuration.GlobalConfiguration

type RecordQueryService struct {
	query *pb.RDBQuery
}

func NewRecordQueryService(query *pb.RDBQuery) *RecordQueryService {
	return &RecordQueryService{query: query}
}


func (s *RecordQueryService) GetRDBRecords(ctx context.Context, query *pb.RDBQuery) (*pb.RecordSet, error) {
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
