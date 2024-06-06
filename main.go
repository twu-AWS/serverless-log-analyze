package main

import (
	"encoding/json"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func transform(event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	log.Printf("current time %s\n", time.Now().String())
	var response events.KinesisFirehoseResponse
	for _, record := range event.Records {

		// Transform data: ToUpper the data
		var transformedRecord events.KinesisFirehoseResponseRecord
		transformedRecord.RecordID = record.RecordID
		transformedRecord.Result = events.KinesisFirehoseTransformedStateOk
		//transformedRecord.Data = []byte(strings.ToUpper(string(record.Data)))
		//wafLogRecord, _ := base64.StdEncoding.DecodeString(string(record.Data))
		var result map[string]interface{}
		err := json.Unmarshal(record.Data, &result)
		if err != nil {
			log.Fatal(err)
			transformedRecord.Result = events.KinesisFirehoseTransformedStateProcessingFailed
			response.Records = append(response.Records, transformedRecord)
			return response, err
		}
		result["timestamp"] = time.Unix(int64(result["timestamp"].(float64))/1000, 0)
		headers := result["httpRequest"].(map[string]any)["headers"].([]interface{})
		for _, header := range headers {
			header, ok := header.(map[string]any)
			if !ok {
				log.Fatal(ok)
				continue
			}
			if slices.Contains(exclude_headers, strings.ToLower(header["name"].(string))) {
				continue
			} else {
				result[header["name"].(string)] = header["value"].(string)
			}
		}
		res, err := json.Marshal(result)
		//var jsonRes = make([]byte, base64.StdEncoding.EncodedLen(len(res)))
		if err != nil {
			log.Fatal(err)
			transformedRecord.Result = events.KinesisFirehoseTransformedStateProcessingFailed
			response.Records = append(response.Records, transformedRecord)
			return response, err
		}

		//base64.StdEncoding.Encode(jsonRes, res)
		//log.Printf("encoded json is %s\n", jsonRes)
		transformedRecord.Data = res
		response.Records = append(response.Records, transformedRecord)
	}
	return response, nil
}

var exclude_headers = []string{"access_token", "cookie", "t_token"}

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	lambda.Start(transform)
}
