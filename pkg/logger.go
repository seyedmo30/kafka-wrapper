package pkg

import (
	"encoding/json"
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
	DBAction            string      `json:"dbAction,omitempty"`
	DBDuration          int         `json:"dbDuration,omitempty"`
	DBRunInTransaction  bool        `json:"dbRunInTransaction,omitempty"`
	APIKey              string      `json:"apiKey,omitempty"`
	APIKeyName          string      `json:"apiKeyName,omitempty"`
	ParentCorrelationID string      `json:"parentCorrelationId,omitempty"`
}

// Message represents the entire JSON structure.
type Message struct {
	Metadata Metadata `json:"metadata"`
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
	logger.Info(mainMessage, mapList...)
}

func MetadataLogger(messageValue []byte) {

	errMsg := ""
	// if err != nil {
	// 	errMsg = err.Error()
	// }
	var messageStruct Message

	err := json.Unmarshal(messageValue, &messageStruct)
	if err != nil {
		logger.Debug("value kafka message cant unmarshal for gogging metadata")
	}

	stdLog := MapStdLogToMetadata(messageStruct.Metadata)

	stdLog.Log(errMsg)
}

func MapStdLogToMetadata(metadata Metadata) StdLog {
	return StdLog{
		Error:               metadata.Error,
		ErrorType:           metadata.ErrorType,
		ErrorStack:          metadata.ErrorStack,
		ErrorDescription:    metadata.ErrorDescription,
		ErrorStatus:         metadata.ErrorStatus,
		ErrorStatusText:     metadata.ErrorStatusText,
		ErrorIssuer:         metadata.ErrorIssuer,
		ErrorMethode:        metadata.ErrorMethode,
		ErrorFileName:       metadata.ErrorFileName,
		ErrorLineNumber:     metadata.ErrorLineNumber,
		UserID:              metadata.UserID,
		Username:            metadata.Username,
		NationalID:          metadata.NationalID,
		Span:                metadata.Span,
		ParentSpan:          metadata.ParentSpan,
		DeviceIP:            metadata.DeviceIP,
		DeviceIsAndroid:     metadata.DeviceIsAndroid,
		DeviceID:            metadata.DeviceID,
		DeviceLoginIP:       metadata.DeviceLoginIP,
		DeviceManufacturer:  metadata.DeviceManufacturer,
		DeviceSystem:        metadata.DeviceSystem,
		DeviceUserAgent:     metadata.DeviceUserAgent,
		DeviceModel:         metadata.DeviceModel,
		AppVersion:          metadata.AppVersion,
		Type:                metadata.Type,
		Payload:             metadata.Payload,
		Message:             metadata.Message,
		ReqMethod:           metadata.ReqMethod,
		ReqURL:              metadata.ReqURL,
		ReqParams:           metadata.ReqParams,
		ReqQuery:            metadata.ReqQuery,
		ReqHeaders:          metadata.ReqHeaders,
		ResHeaders:          metadata.ResHeaders,
		ResDuration:         metadata.ResDuration,
		ResBody:             metadata.ResBody,
		ResStatus:           metadata.ResStatus,
		ResStatusText:       metadata.ResStatusText,
		Process:             metadata.Process,
		KafkaTime:           metadata.KafkaTime,
		KafkaTopic:          metadata.KafkaTopic,
		KafkaProcess:        metadata.KafkaProcess,
		KafkaDuration:       metadata.KafkaDuration,
		DBModel:             metadata.DBModel,
		DBAction:            metadata.DBAction,
		DBDuration:          metadata.DBDuration,
		DBRunInTransaction:  metadata.DBRunInTransaction,
		APIKey:              metadata.APIKey,
		APIKeyName:          metadata.APIKeyName,
		ParentCorrelationID: metadata.ParentCorrelationID,
	}
}
