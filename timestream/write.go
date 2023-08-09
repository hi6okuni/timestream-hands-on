package timestream

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/net/http2"
)

// 正常書き込みの確認

// 明示的なschema定義は無い
// カラムは追加できるが、一回作られたカラムのタイプは変更できない(ex. Double -> Bigint)
// DimensionとTimeが重複判定の要素であることの確認
// versionを使って、上書きUpsertすることができる
// 未来の時刻15分以上は入らない

/*
SELECT * FROM "realtime-test"."hands_on"
WHERE time between ago(5h) and now()
AND name = 'YOUR_NAME'
ORDER BY time DESC LIMIT 10
*/

func Write() {
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		// Using DefaultTransport values for other parameters: https://golang.org/pkg/net/http/#RoundTripper
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			DualStack: true,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// So client makes HTTP/2 requests
	http2.ConfigureTransport(tr)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sess.Config.Region = aws.String("ap-northeast-1")

	writeSvc := timestreamwrite.New(sess)

	now := time.Now()
	currentTimeInSeconds := now.UnixNano()

	// layout := "2006-01-02 15:04:05"
	// str := "2023-08-09 03:00:00"
	// specificTime, err := time.Parse(layout, str)
	// if err != nil {
	// 	fmt.Println("Error:", err)
	// 	return
	// }

	// version := time.Now().Round(time.Millisecond).UnixNano() / 1e6

	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String("realtime-test"),
		TableName:    aws.String("hands_on"),
		Records: []*timestreamwrite.Record{
			{
				Dimensions: []*timestreamwrite.Dimension{
					{
						Name:  aws.String("name"),
						Value: aws.String("honda"), // write your name
					},
				},
				MeasureName:      aws.String("body_temperature"),
				MeasureValueType: aws.String("MULTI"),
				Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
				TimeUnit:         aws.String(timestreamwrite.TimeUnitNanoseconds),
				// Version:          &version,
				MeasureValues: []*timestreamwrite.MeasureValue{
					{
						Name:  aws.String("occupation"),
						Value: aws.String("engineer"),
						Type:  aws.String(timestreamwrite.MeasureValueTypeVarchar),
					},
					{
						Name:  aws.String("degree"),
						Value: aws.String("37.2"),
						Type:  aws.String(timestreamwrite.MeasureValueTypeDouble),
					},
					{
						Name:  aws.String("blood pressure"),
						Value: aws.String("140"),
						Type:  aws.String(timestreamwrite.MeasureValueTypeDouble),
					},
				},
			},
		},
	}

	_, err := writeSvc.WriteRecords(writeRecordsInput)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records is successful")
	}
}
