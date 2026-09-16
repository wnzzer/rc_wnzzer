// Package config 从环境变量读取配置。
//
// 不引入配置解析库（决策 D-001）：一个服务的配置项只有十几个，env 足够；
// 引入 viper/yaml 这类库带来的是多环境文件、覆盖优先级、热加载等一整套
// 本项目用不到的语义。
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是 notifyd 的全部运行时配置。
type Config struct {
	Addr     string
	DataDir  string
	KeysFile string

	Workers         int
	HostConcurrency int
	QueueMax        int

	MaxAttempts int
	BaseBackoff time.Duration
	BackoffCap  time.Duration
	Timeout     time.Duration
	Deadline    time.Duration
	DialTimeout time.Duration

	Retention        time.Duration
	MaintainInterval time.Duration
	SkewTolerance    time.Duration
	ShutdownGrace    time.Duration

	CompactMinBytes  int64
	CompactLiveRatio float64
	StripThreshold   int

	AllowPrivateHosts bool
}

// Load 读取环境变量，未设置的项用默认值。
//
// 默认值不是抄来的，是算出来的：MaxAttempts=24 配合 BackoffCap=1h，
// 最坏覆盖 12.1h、期望覆盖 6.1h 的对端宕机，这才对得上「吸收小时级宕机」
// 这个承诺（见 spec §6.1 的推导表）。
func Load() (Config, error) {
	c := Config{
		Addr:              env("NOTIFY_ADDR", ":8080"),
		DataDir:           env("NOTIFY_DATA_DIR", "./data"),
		KeysFile:          env("NOTIFY_KEYS_FILE", "./keys"),
		Workers:           envInt("NOTIFY_WORKERS", 32),
		HostConcurrency:   envInt("NOTIFY_HOST_CONCURRENCY", 8),
		QueueMax:          envInt("NOTIFY_QUEUE_MAX", 100_000),
		MaxAttempts:       envInt("NOTIFY_MAX_ATTEMPTS", 24),
		BaseBackoff:       envDur("NOTIFY_BACKOFF_BASE", time.Second),
		BackoffCap:        envDur("NOTIFY_BACKOFF_CAP", time.Hour),
		Timeout:           envDur("NOTIFY_TIMEOUT", 5*time.Second),
		Deadline:          envDur("NOTIFY_DEADLINE", 24*time.Hour),
		DialTimeout:       envDur("NOTIFY_DIAL_TIMEOUT", 5*time.Second),
		Retention:         envDur("NOTIFY_RETENTION", 7*24*time.Hour),
		MaintainInterval:  envDur("NOTIFY_MAINTAIN_INTERVAL", time.Minute),
		SkewTolerance:     envDur("NOTIFY_SKEW_TOLERANCE", 5*time.Minute),
		ShutdownGrace:     envDur("NOTIFY_SHUTDOWN_GRACE", 30*time.Second),
		CompactMinBytes:   int64(envInt("NOTIFY_COMPACT_MIN_BYTES", 64<<20)),
		CompactLiveRatio:  envFloat("NOTIFY_COMPACT_LIVE_RATIO", 0.5),
		StripThreshold:    envInt("NOTIFY_COMPACT_STRIP_THRESHOLD", 1000),
		AllowPrivateHosts: envBool("NOTIFY_ALLOW_PRIVATE_HOSTS", false),
	}
	if c.Workers < 1 {
		return c, fmt.Errorf("config: NOTIFY_WORKERS 必须 >= 1")
	}
	if c.HostConcurrency < 1 {
		return c, fmt.Errorf("config: NOTIFY_HOST_CONCURRENCY 必须 >= 1")
	}
	if c.MaxAttempts < 1 {
		return c, fmt.Errorf("config: NOTIFY_MAX_ATTEMPTS 必须 >= 1")
	}
	return c, nil
}

// LoadKeys 从文件读取调用方密钥，格式为每行 `key_id:secret`，# 开头为注释。
//
// 用文件而不是环境变量（spec §4.1）：环境变量会出现在 ps 输出、崩溃转储和
// 容器 inspect 里。文件至少可以靠权限位把它挡住。
func LoadKeys(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: 打开密钥文件: %w", err)
	}
	defer f.Close()

	if st, err := f.Stat(); err == nil && st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("config: 密钥文件 %s 权限过宽 (%04o)，应为 0600", path, st.Mode().Perm())
	}

	keys := make(map[string]string)
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		id, secret, ok := strings.Cut(s, ":")
		if !ok || id == "" || secret == "" {
			return nil, fmt.Errorf("config: 密钥文件第 %d 行格式应为 key_id:secret", line)
		}
		keys[id] = secret
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("config: 读取密钥文件: %w", err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("config: 密钥文件 %s 中没有有效条目", path)
	}
	return keys, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
