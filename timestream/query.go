package timestream

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

/*
query: SELECT * FROM "realtime-test"."subscription_measurement_histories" WHERE created_at between ago(2d) and now() ORDER BY id DESC
Number of Rows: 0
Number of Rows: 1380
Number of Rows: 1388
Number of Rows: 1376
Number of Rows: 1375
Number of Rows: 1375
Number of Rows: 1384
Number of Rows: 1376
Number of Rows: 1377
Number of Rows: 1387
Number of Rows: 1378
Number of Rows: 1376
Number of Rows: 1377
Number of Rows: 1383
Number of Rows: 1377
Number of Rows: 463
CSV file created successfully
*/

// Console画面でQuery
// go sdkからQuery
// https://github.com/awslabs/amazon-timestream-tools/blob/mainline/sample_apps/goV2/utils/query-common.go#L116

func Query() {
	//Create a new Timestream Query client
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sess.Config.Region = aws.String("ap-northeast-1")
	svc := timestreamquery.New(sess)

	// Set the query input parameters
	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String("SELECT * FROM \"realtime-test\".\"subscription_measurement_histories\" WHERE created_at between ago(1d) and now() ORDER BY id DESC"),
	}
	fmt.Printf("query: %s\n", *queryInput.QueryString)

	// Create a new CSV file
	file, err := os.Create("output.csv")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Create a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row to the CSV file
	header := []string{"id,subscription_id, time"}
	writer.Write(header)

	for {
		queryResponse, err := svc.Query(queryInput)
		if err != nil {
			fmt.Printf("Error while querying the query %s : %s\n", queryInput, err.Error())
			return
		}

		msg := fmt.Sprintf("Number of Rows: %d\n", len(queryResponse.Rows))
		fmt.Print(msg)

		// Write the data rows to the CSV file
		for _, row := range queryResponse.Rows {
			idStr := aws.StringValue(row.Data[17].ScalarValue)
			subscriptionIDStr := aws.StringValue(row.Data[0].ScalarValue)
			timeStr := aws.StringValue(row.Data[2].ScalarValue)

			data := []string{idStr, subscriptionIDStr, timeStr}
			writer.Write(data)
		}
		if queryResponse.NextToken == nil {
			break
		}
		queryInput.NextToken = queryResponse.NextToken
	}

	fmt.Println("CSV file created successfully")
}
