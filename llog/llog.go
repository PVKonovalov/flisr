// Created by Pavel Konovalov pkonovalov@orxagrid.com
//
// Log with standard different logging levels
//

package llog

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	Ldate         = log.Ldate             // the date in the local time zone: 2009/01/23
	Ltime         = log.Ltime             // the time in the local time zone: 01:23:23
	Lmicroseconds = log.Lmicroseconds     // microsecond resolution: 01:23:23.123123.  assumes Ltime.
	Llongfile     = log.Llongfile         // full file name and line number: /a/b/c/d.go:23
	Lshortfile    = log.Lshortfile        // final file name element and line number: d.go:23. overrides Llongfile
	LUTC          = log.LUTC              // if Ldate or Ltime is set, use UTC rather than the local time zone
	Lmsgprefix    = log.Lmsgprefix        // move the "prefix" from the beginning of the line to before the message
	LstdFlags     = log.Ldate | log.Ltime // initial values for the standard logger
)

// Level type
type Level uint32

type LevelLog struct {
	Debug    *log.Logger // Just about anything
	Info     *log.Logger // Important information
	Warning  *log.Logger // Be concerned
	Error    *log.Logger // Critical problem
	curLevel Level
}

// NewLevelLog constructor, flag: log.Ldate|log.Ltime|log.Lmicroseconds
func NewLevelLog(flag int) *LevelLog {
	return &LevelLog{

		Warning:  log.New(os.Stderr, "WARNING: ", flag),
		Debug:    log.New(os.Stderr, "DEBUG: ", flag),
		Info:     log.New(os.Stderr, "INFO: ", flag),
		Error:    log.New(os.Stderr, "ERROR: ", flag),
		curLevel: InfoLevel,
	}
}

// These are the different logging levels
const (
	ErrorLevel Level = iota
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
)

// Convert the Level to a string. E.g. DebugLevel becomes "debug"
func (level Level) String() string {
	if b, err := level.MarshalText(); err == nil {
		return string(b)
	} else {
		return "unknown"
	}
}

func (level Level) UpperString() string {
	return strings.ToUpper(level.String())
}

// ParseLevel takes a string level and returns the log level constant
func ParseLevel(level string, levelDefault Level) (Level, error) {
	switch strings.ToLower(level) {
	case "error":
		return ErrorLevel, nil
	case "warn", "warning":
		return WarnLevel, nil
	case "info":
		return InfoLevel, nil
	case "debug":
		return DebugLevel, nil
	default:
		return levelDefault, fmt.Errorf("nwrong log level: %q uses: %q", level, levelDefault)
	}
}

func (level Level) MarshalText() ([]byte, error) {
	switch level {
	case DebugLevel:
		return []byte("debug"), nil
	case InfoLevel:
		return []byte("info"), nil
	case WarnLevel:
		return []byte("warning"), nil
	case ErrorLevel:
		return []byte("error"), nil
	}

	return nil, fmt.Errorf("not a valid log level %d", level)
}

// UnmarshalText implements encoding.TextUnmarshaler
func (level *Level) UnmarshalText(text []byte) error {
	l, err := ParseLevel(string(text), WarnLevel)
	if err != nil {
		return err
	}

	*level = l

	return nil
}

func (l *LevelLog) SetLevel(level Level) {
	l.curLevel = level
}

func (l *LevelLog) Errorf(format string, args ...interface{}) {
	l.Error.Printf(format, args...)
}

func (l *LevelLog) Fatalf(format string, args ...interface{}) {
	l.Error.Fatalf(format, args...)
}

func (l *LevelLog) Infof(format string, args ...interface{}) {
	if l.IsLevelEnabled(InfoLevel) {
		l.Info.Printf(format, args...)
	}
}

func (l *LevelLog) Debugf(format string, args ...interface{}) {
	if l.IsLevelEnabled(DebugLevel) {
		l.Debug.Printf(format, args...)
	}
}

func (l *LevelLog) Warnf(format string, args ...interface{}) {
	if l.IsLevelEnabled(WarnLevel) {
		l.Warning.Printf(format, args...)
	}
}

// IsLevelEnabled checks if the log level of the logger is greater than the level param
func (l *LevelLog) IsLevelEnabled(level Level) bool {
	return l.curLevel >= level
}

// GetLevel returns the current logger level.
func (l *LevelLog) GetLevel() Level {
	return l.curLevel
}
