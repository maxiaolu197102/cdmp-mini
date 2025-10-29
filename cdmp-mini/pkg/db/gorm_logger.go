package db

import (
	"context"
	"fmt"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"gorm.io/gorm/logger"
)

const (
	defaultSlowQueryThreshold = 200 * time.Millisecond
)

type gormLoggerAdapter struct {
	config logger.Config
}

func newGormLogger(opts *Options) logger.Interface {
	cfg := logger.Config{
		IgnoreRecordNotFoundError: true,
		Colorful:                  false,
		SlowThreshold:             opts.SlowQueryThreshold,
		LogLevel:                  toGormLogLevel(opts.LogLevel),
	}

	if cfg.SlowThreshold <= 0 {
		cfg.SlowThreshold = defaultSlowQueryThreshold
	}

	return &gormLoggerAdapter{config: cfg}
}

func (g *gormLoggerAdapter) LogMode(level logger.LogLevel) logger.Interface {
	clone := *g
	clone.config.LogLevel = level
	return &clone
}

func (g *gormLoggerAdapter) Info(ctx context.Context, msg string, args ...interface{}) {
	if g == nil || g.config.LogLevel < logger.Info {
		return
	}
	log.Infof("[gorm] %s", fmt.Sprintf(msg, args...))
}

func (g *gormLoggerAdapter) Warn(ctx context.Context, msg string, args ...interface{}) {
	if g == nil || g.config.LogLevel < logger.Warn {
		return
	}
	log.Warnf("[gorm] %s", fmt.Sprintf(msg, args...))
}

func (g *gormLoggerAdapter) Error(ctx context.Context, msg string, args ...interface{}) {
	if g == nil || g.config.LogLevel < logger.Error {
		return
	}
	log.Errorf("[gorm] %s", fmt.Sprintf(msg, args...))
}

func (g *gormLoggerAdapter) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if g == nil || g.config.LogLevel == logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	rowCount := rows
	if rowCount < 0 {
		rowCount = 0
	}

	switch {
	case err != nil && g.config.LogLevel >= logger.Error:
		// 过滤 record not found，降级为 INFO
		if err.Error() == "record not found" {
			if g.config.LogLevel >= logger.Info {
				log.Infof("[gorm] record not found elapsed=%s rows=%d sql=%s", elapsed, rowCount, sql)
			}
		} else {
			// 真正的错误仍然记录为 ERROR
			log.Errorf("[gorm] error=%v elapsed=%s rows=%d sql=%s", err, elapsed, rowCount, sql)
		}
	case g.config.SlowThreshold > 0 && elapsed > g.config.SlowThreshold && g.config.LogLevel >= logger.Warn:
		log.Warnf("[gorm] slow query >= %s elapsed=%s rows=%d sql=%s", g.config.SlowThreshold, elapsed, rowCount, sql)
	case g.config.LogLevel >= logger.Info:
		// 普通查询也改为 INFO 级别
		log.Infof("[gorm] query elapsed=%s rows=%d sql=%s", elapsed, rowCount, sql)
	}
}

func toGormLogLevel(level int) logger.LogLevel {
	switch level {
	case 0:
		return logger.Silent
	case 1:
		return logger.Error
	case 2:
		return logger.Warn
	case 3:
		return logger.Info
	default:
		return logger.Info
	}
}
