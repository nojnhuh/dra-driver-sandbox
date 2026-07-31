//go:build e2e

package e2e

import (
	"fmt"
	"strings"

	"k8s.io/klog/v2"
	"sigs.k8s.io/kind/pkg/log"
)

type kindLogAdapter struct {
	klog.Logger
}

// Error implements [log.Logger].
func (k kindLogAdapter) Error(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Error(nil, strings.TrimSpace(message))
}

// Errorf implements [log.Logger].
func (k kindLogAdapter) Errorf(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Error(nil, fmt.Sprintf(strings.TrimSpace(format), args...))
}

// Warn implements [log.Logger].
func (k kindLogAdapter) Warn(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(strings.TrimSpace(message))
}

// Warnf implements [log.Logger].
func (k kindLogAdapter) Warnf(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(fmt.Sprintf(strings.TrimSpace(format), args...))
}

// V implements [log.Logger].
func (k kindLogAdapter) V(v log.Level) log.InfoLogger {
	return kindInfoLogAdapter{k.Logger.V(int(v))}
}

type kindInfoLogAdapter struct {
	klog.Logger
}

// Info implements [log.InfoLogger].
func (k kindInfoLogAdapter) Info(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(strings.TrimSpace(message))
}

// Infof implements [log.InfoLogger].
func (k kindInfoLogAdapter) Infof(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(fmt.Sprintf(strings.TrimSpace(format), args...))
}
