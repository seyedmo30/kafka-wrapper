package pkg

import (
	"os"
	"time"

	"log/slog"

	"github.com/lmittmann/tint"
)

var logger *slog.Logger

func init() {

	// for product
	// h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	// 	Level:     slog.LevelError,
	// })

	// for develop
	h := tint.NewHandler(os.Stderr, &tint.Options{
		AddSource:  true,
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})

	logger = slog.New(h)

}

type LogSTD struct {
	Error               interface{} `json:"error,omitempty"`
	ErrorType           string      `json:"errorType,omitempty"`
	ErrorStack          interface{} `json:"errorStack,omitempty"`
	ErrorDescription    string      `json:"errorDescription,omitempty"`
	ErrorStatus         int         `json:"errorStatus,omitempty"`
	ErrorStatusText     interface{} `json:"errorStatusText,omitempty"`
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
