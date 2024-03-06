package kafkawrapper

import (
	"os"
	"sync"

	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *slog.Logger

func init() {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger = slog.New(h)

}

var logsOnce sync.Once

var logsInstance *zap.Logger

func logs() *zap.Logger {

	logsOnce.Do(func() {

		config := zap.NewProductionEncoderConfig()
		config.EncodeTime = zapcore.ISO8601TimeEncoder
		consoleEncoder := zapcore.NewConsoleEncoder(config)
		level := getLogLevel()
		exitLevel := zapcore.FatalLevel
		core := zapcore.NewTee(
			zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
		)

		logsInstance = zap.New(core, zap.AddCaller(), zap.AddStacktrace(exitLevel))

	})

	return logsInstance
}

func getLogLevel() zapcore.Level {
	level := os.Getenv("LOG_LEVEL")
	defaultLogLevel := zapcore.DebugLevel

	if level == "debug" || level == "DEBUG" {
		defaultLogLevel = zapcore.DebugLevel
	}

	if level == "info" || level == "INFO" {
		defaultLogLevel = zapcore.InfoLevel
	}

	if level == "warn" || level == "WARN" {
		defaultLogLevel = zapcore.WarnLevel
	}

	if level == "ERROR" || level == "error" {
		defaultLogLevel = zapcore.ErrorLevel
	}

	if level == "PANIC" || level == "panic" {
		defaultLogLevel = zapcore.PanicLevel
	}

	if level == "FATAL" || level == "fatal" {
		defaultLogLevel = zapcore.FatalLevel
	}

	return defaultLogLevel
}
