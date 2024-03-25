package pkg

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"log/slog"
)

var logger *slog.Logger

func init() {

	// for product
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	// for develop
	// h := tint.NewHandler(os.Stderr, &tint.Options{
	// 	AddSource:  true,
	// 	Level:      slog.LevelDebug,
	// 	TimeFormat: time.Kitchen,
	// })

	logger = slog.New(h)

}

type StdLog struct {
	Error               string      `json:"error,omitempty"`
	ErrorType           string      `json:"errorType,omitempty"`
	ErrorStack          string      `json:"errorStack,omitempty"`
	ErrorDescription    string      `json:"errorDescription,omitempty"`
	ErrorStatus         int         `json:"errorStatus,omitempty"`
	ErrorStatusText     string      `json:"errorStatusText,omitempty"`
	ErrorIssuer         string      `json:"errorIssuer,omitempty"`
	ErrorMethode        string      `json:"errorMethode,omitempty"`
	ErrorFileName       string      `json:"errorFileName,omitempty"`
	ErrorLineNumber     string      `json:"errorLineNumber,omitempty"`
	UserID              string      `json:"userId,omitempty"`
	Username            string      `json:"username,omitempty"`
	NationalID          string      `json:"nationalId,omitempty"`
	Span                string      `json:"span,omitempty"`
	ParentSpan          string      `json:"parentSpan,omitempty"`
	DeviceIP            string      `json:"deviceIp,omitempty"`
	DeviceIsAndroid     bool        `json:"deviceIsAndroid,omitempty"`
	DeviceID            string      `json:"deviceId,omitempty"`
	DeviceLoginIP       string      `json:"deviceLoginIp,omitempty"`
	DeviceManufacturer  string      `json:"deviceManufacturer,omitempty"`
	DeviceSystem        int         `json:"deviceSystem,omitempty"`
	DeviceUserAgent     string      `json:"deviceUserAgent,omitempty"`
	DeviceModel         string      `json:"deviceModel,omitempty"`
	AppVersion          string      `json:"appVersion,omitempty"`
	Type                string      `json:"type"`
	Payload             string      `json:"payload,omitempty"`
	Message             string      `json:"message"`
	ReqMethod           string      `json:"reqMethod,omitempty"`
	ReqURL              string      `json:"reqUrl,omitempty"`
	ReqParams           string      `json:"reqParams,omitempty"`
	ReqQuery            string      `json:"reqQuery,omitempty"`
	ReqHeaders          interface{} `json:"reqHeaders,omitempty"`
	ResHeaders          interface{} `json:"resHeaders,omitempty"`
	ResDuration         interface{} `json:"resDuration,omitempty"`
	ResBody             interface{} `json:"resBody,omitempty"`
	ResStatus           int         `json:"resStatus,omitempty"`
	ResStatusText       string      `json:"resStatusText,omitempty"`
	Process             interface{} `json:"process"`
	KafkaTime           string      `json:"kafkaTime,omitempty"`
	KafkaTopic          string      `json:"kafkaTopic,omitempty"`
	KafkaProcess        interface{} `json:"kafkaProcess"`
	KafkaDuration       int         `json:"kafkaDuration,omitempty"`
	DBModel             string      `json:"dbModel,omitempty"`
	DBArgs              string      `json:"dbArgs,omitempty"`
	DBAction            string      `json:"dbAction,omitempty"`
	DBDuration          int         `json:"dbDuration,omitempty"`
	DBRunInTransaction  bool        `json:"dbRunInTransaction,omitempty"`
	APIKey              string      `json:"apiKey,omitempty"`
	APIKeyName          string      `json:"apiKeyName,omitempty"`
	ParentCorrelationID string      `json:"parentCorrelationId,omitempty"`
}

type Metadata struct {
	Error               string      `json:"error,omitempty"`
	ErrorType           string      `json:"errorType,omitempty"`
	ErrorStack          string      `json:"errorStack,omitempty"`
	ErrorDescription    string      `json:"errorDescription,omitempty"`
	ErrorStatus         int         `json:"errorStatus,omitempty"`
	ErrorStatusText     string      `json:"errorStatusText,omitempty"`
	ErrorIssuer         string      `json:"errorIssuer,omitempty"`
	ErrorMethode        string      `json:"errorMethode,omitempty"`
	ErrorFileName       string      `json:"errorFileName,omitempty"`
	ErrorLineNumber     string      `json:"errorLineNumber,omitempty"`
	UserID              string      `json:"userId,omitempty"`
	Username            string      `json:"username,omitempty"`
	NationalID          string      `json:"nationalId,omitempty"`
	Span                string      `json:"span,omitempty"`
	ParentSpan          string      `json:"parentSpan,omitempty"`
	DeviceIP            string      `json:"deviceIp,omitempty"`
	DeviceIsAndroid     bool        `json:"deviceIsAndroid,omitempty"`
	DeviceID            string      `json:"deviceId,omitempty"`
	DeviceLoginIP       string      `json:"deviceLoginIp,omitempty"`
	DeviceManufacturer  string      `json:"deviceManufacturer,omitempty"`
	DeviceSystem        int         `json:"deviceSystem,omitempty"`
	DeviceUserAgent     string      `json:"deviceUserAgent,omitempty"`
	DeviceModel         string      `json:"deviceModel,omitempty"`
	AppVersion          string      `json:"appVersion,omitempty"`
	Type                string      `json:"type"`
	Payload             string      `json:"payload,omitempty"`
	Message             string      `json:"message"`
	ReqMethod           string      `json:"reqMethod,omitempty"`
	ReqURL              string      `json:"reqUrl,omitempty"`
	ReqParams           string      `json:"reqParams,omitempty"`
	ReqQuery            string      `json:"reqQuery,omitempty"`
	ReqHeaders          interface{} `json:"reqHeaders,omitempty"`
	ResHeaders          interface{} `json:"resHeaders,omitempty"`
	ResDuration         interface{} `json:"resDuration,omitempty"`
	ResBody             interface{} `json:"resBody,omitempty"`
	ResStatus           int         `json:"resStatus,omitempty"`
	ResStatusText       string      `json:"resStatusText,omitempty"`
	Process             interface{} `json:"process"`
	KafkaTime           string      `json:"kafkaTime,omitempty"`
	KafkaTopic          string      `json:"kafkaTopic,omitempty"`
	KafkaProcess        interface{} `json:"kafkaProcess"`
	KafkaDuration       int         `json:"kafkaDuration,omitempty"`
	DBModel             string      `json:"dbModel,omitempty"`
	DBArgs              string      `json:"dbArgs,omitempty"`
	DBAction            string      `json:"dbAction,omitempty"`
	DBDuration          int         `json:"dbDuration,omitempty"`
	DBRunInTransaction  bool        `json:"dbRunInTransaction,omitempty"`
	APIKey              string      `json:"apiKey,omitempty"`
	APIKeyName          string      `json:"apiKeyName,omitempty"`
	ParentCorrelationID string      `json:"parentCorrelationId,omitempty"`
}

// Payload represents the payload fields in the JSON.
type Payload struct {
	Receptor string `json:"receptor"`
	Token    string `json:"token"`
	Template string `json:"template"`
}

// Message represents the entire JSON structure.
type Message struct {
	Metadata Metadata `json:"metadata"`
	Payload  Payload  `json:"payload"`
}

func (l *StdLog) Log(mainMessage string) {
	v := reflect.ValueOf(l).Elem()
	t := reflect.TypeOf(l).Elem()
	mapList := make([]interface{}, 0, 10)

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		tag = strings.Split(tag, ",")[0]
		value := v.Field(i).Interface()

		// Check if the field is not the zero value for its type
		if !reflect.DeepEqual(value, reflect.Zero(field.Type).Interface()) {
			mapList = append(mapList, tag, value)
		}
	}
	logger.Error(mainMessage, mapList...)
}

func MetadataLogger(messageValue []byte) {

	errMsg := ""
	// if err != nil {
	// 	errMsg = err.Error()
	// }
	fmt.Println("ss")
	var messageStruct Message

	err := json.Unmarshal(messageValue, &messageStruct)
	if err != nil {
		logger.Debug("value kafka message cant unmarshal for gogging metadata")
	}

	stdLog := MapStdLogToMetadata(messageStruct.Metadata)

	stdLog.Log(errMsg)
}

func MapStdLogToMetadata(stdLog Metadata) StdLog {
	return StdLog{
		Error:               stdLog.Error,
		ErrorType:           stdLog.ErrorType,
		ErrorStack:          stdLog.ErrorStack,
		ErrorDescription:    stdLog.ErrorDescription,
		ErrorStatus:         stdLog.ErrorStatus,
		ErrorStatusText:     stdLog.ErrorStatusText,
		ErrorIssuer:         stdLog.ErrorIssuer,
		ErrorMethode:        stdLog.ErrorMethode,
		ErrorFileName:       stdLog.ErrorFileName,
		ErrorLineNumber:     stdLog.ErrorLineNumber,
		UserID:              stdLog.UserID,
		Username:            stdLog.Username,
		NationalID:          stdLog.NationalID,
		Span:                stdLog.Span,
		ParentSpan:          stdLog.ParentSpan,
		DeviceIP:            stdLog.DeviceIP,
		DeviceIsAndroid:     stdLog.DeviceIsAndroid,
		DeviceID:            stdLog.DeviceID,
		DeviceLoginIP:       stdLog.DeviceLoginIP,
		DeviceManufacturer:  stdLog.DeviceManufacturer,
		DeviceSystem:        stdLog.DeviceSystem,
		DeviceUserAgent:     stdLog.DeviceUserAgent,
		DeviceModel:         stdLog.DeviceModel,
		AppVersion:          stdLog.AppVersion,
		Type:                stdLog.Type,
		Payload:             stdLog.Payload,
		Message:             stdLog.Message,
		ReqMethod:           stdLog.ReqMethod,
		ReqURL:              stdLog.ReqURL,
		ReqParams:           stdLog.ReqParams,
		ReqQuery:            stdLog.ReqQuery,
		ReqHeaders:          stdLog.ReqHeaders,
		ResHeaders:          stdLog.ResHeaders,
		ResDuration:         stdLog.ResDuration,
		ResBody:             stdLog.ResBody,
		ResStatus:           stdLog.ResStatus,
		ResStatusText:       stdLog.ResStatusText,
		Process:             stdLog.Process,
		KafkaTime:           stdLog.KafkaTime,
		KafkaTopic:          stdLog.KafkaTopic,
		KafkaProcess:        stdLog.KafkaProcess,
		KafkaDuration:       stdLog.KafkaDuration,
		DBModel:             stdLog.DBModel,
		DBArgs:              stdLog.DBArgs,
		DBAction:            stdLog.DBAction,
		DBDuration:          stdLog.DBDuration,
		DBRunInTransaction:  stdLog.DBRunInTransaction,
		APIKey:              stdLog.APIKey,
		APIKeyName:          stdLog.APIKeyName,
		ParentCorrelationID: stdLog.ParentCorrelationID,
	}
}
