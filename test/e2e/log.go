//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"k8s.io/klog/v2"
	"sigs.k8s.io/kind/pkg/log"
)

type kindLogAdapter struct {
	klog.Logger
}

// Error implements [log.Logger].
func (k kindLogAdapter) Error(message string) {
	k.Logger.WithCallDepth(2).Error(nil, strings.TrimSpace(message))
}

// Errorf implements [log.Logger].
func (k kindLogAdapter) Errorf(format string, args ...any) {
	k.Logger.WithCallDepth(2).Error(nil, fmt.Sprintf(strings.TrimSpace(format), args...))
}

// Warn implements [log.Logger].
func (k kindLogAdapter) Warn(message string) {
	k.Logger.WithCallDepth(2).Info(strings.TrimSpace(message))
}

// Warnf implements [log.Logger].
func (k kindLogAdapter) Warnf(format string, args ...any) {
	k.Logger.WithCallDepth(2).Info(fmt.Sprintf(strings.TrimSpace(format), args...))
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
	k.Logger.WithCallDepth(2).Info(strings.TrimSpace(message))
}

// Infof implements [log.InfoLogger].
func (k kindInfoLogAdapter) Infof(format string, args ...any) {
	k.Logger.WithCallDepth(2).Info(fmt.Sprintf(strings.TrimSpace(format), args...))
}

func helmLogger(ctx context.Context, t *testing.T) slog.Handler {
	level := slog.LevelInfo
	if klog.FromContext(ctx).V(6).Enabled() {
		level = slog.LevelDebug
	}
	return slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: level})
}
