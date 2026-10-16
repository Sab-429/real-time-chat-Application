package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Part string

const (
	PartDB    Part = "database"
	PartRedis Part = "redis"
	PartKafka Part = "kafka"
	PartJWT   Part = "jwt"
)

type Config struct {
	Env      string // dev | prod
	LogLevel string

	DatabaseURL string

	RedisAddrs    []string
	RedisPassword string
	RedisTLS      bool

	KafkaBrokers     []string
	KafkaReplication int

	JWTSecret      string
	AllowedOrigins []string

	APIAddr     string
	GatewayAddr string
	HealthAddr  string // workers and relay; "" disables the health server

	ShutdownTimeout time.Duration
	ShutdownDelay   time.Duration // gateway: time to let the load balancer drain before closing sockets

	FCMCredentialsFile  string // "" in prod = Application Default Credentials
	PushIncludePreview  bool
	WebhookAllowPrivate bool // dev only: allow webhook targets on private networks
}

func (c *Config) IsDev() bool { return c.Env == "dev" }

type loader struct{ errs []string }

func (l *loader) str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (l *loader) boolean(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, key+" must be a boolean")
		return def
	}
	return b
}

func (l *loader) integer(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, key+" must be an integer")
		return def
	}
	return n
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, key+" must be a duration like 10s")
		return def
	}
	return d
}

func (l *loader) list(key, def string) []string {
	raw := l.str(key, def)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func ifDev(dev bool, v string) string {
	if dev {
		return v
	}
	return ""
}

// Load reads configuration from the environment (and an optional .env file;
// real environment variables always win). Defaults exist for dev only, so
// production must set everything explicitly.
func Load() (*Config, error) {
	_ = godotenv.Load()

	l := &loader{}
	c := &Config{Env: strings.ToLower(l.str("ENV", "dev"))}
	if c.Env != "dev" && c.Env != "prod" {
		return nil, fmt.Errorf("ENV must be 'dev' or 'prod', got %q", c.Env)
	}
	dev := c.IsDev()

	c.LogLevel = l.str("LOG_LEVEL", "info")
	c.DatabaseURL = l.str("DATABASE_URL", ifDev(dev, "postgres://postgres:postgres@localhost:5432/chat?sslmode=disable"))
	c.RedisAddrs = l.list("REDIS_ADDRS", ifDev(dev, "localhost:6379"))
	c.RedisPassword = l.str("REDIS_PASSWORD", "")
	c.RedisTLS = l.boolean("REDIS_TLS", false)
	c.KafkaBrokers = l.list("KAFKA_BROKERS", ifDev(dev, "localhost:9092"))
	repl := 1
	if !dev {
		repl = 3
	}
	c.KafkaReplication = l.integer("KAFKA_REPLICATION", repl)
	c.JWTSecret = l.str("JWT_SECRET", ifDev(dev, "dev-only-secret-change-me-0123456789abcdef"))
	c.AllowedOrigins = l.list("ALLOWED_ORIGINS", ifDev(dev, "http://localhost:3000"))
	c.APIAddr = l.str("API_ADDR", ":8080")
	c.GatewayAddr = l.str("GATEWAY_ADDR", ":8081")
	health := ""
	if !dev {
		health = ":9090"
	}
	c.HealthAddr = l.str("HEALTH_ADDR", health)
	c.ShutdownTimeout = l.duration("SHUTDOWN_TIMEOUT", 25*time.Second)
	c.ShutdownDelay = l.duration("SHUTDOWN_DELAY", 5*time.Second)
	c.FCMCredentialsFile = l.str("FCM_CREDENTIALS_FILE", "")
	c.PushIncludePreview = l.boolean("PUSH_INCLUDE_PREVIEW", false)
	c.WebhookAllowPrivate = l.boolean("WEBHOOK_ALLOW_PRIVATE", false)

	if !dev {
		for _, o := range c.AllowedOrigins {
			if o == "*" {
				l.errs = append(l.errs, "ALLOWED_ORIGINS must not contain '*' in prod")
			}
		}
		if c.WebhookAllowPrivate {
			l.errs = append(l.errs, "WEBHOOK_ALLOW_PRIVATE must be false in prod (SSRF protection)")
		}
	}
	if len(l.errs) > 0 {
		return nil, fmt.Errorf("invalid configuration: %s", strings.Join(l.errs, "; "))
	}
	return c, nil
}

// Require checks the settings a particular binary cannot run without.
func (c *Config) Require(parts ...Part) error {
	var missing []string
	for _, p := range parts {
		switch p {
		case PartDB:
			if c.DatabaseURL == "" {
				missing = append(missing, "DATABASE_URL")
			}
		case PartRedis:
			if len(c.RedisAddrs) == 0 {
				missing = append(missing, "REDIS_ADDRS")
			}
		case PartKafka:
			if len(c.KafkaBrokers) == 0 {
				missing = append(missing, "KAFKA_BROKERS")
			}
		case PartJWT:
			if len(c.JWTSecret) < 32 {
				missing = append(missing, "JWT_SECRET (min 32 characters)")
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing or invalid configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}