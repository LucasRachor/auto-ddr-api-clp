package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCAddr string
	CLPHost  string
	CLPPort  uint16
	LogLevel string

	// Fila de comandos
	QueueDBPath  string
	AckTimeoutMS int
	MaxRetries   int
	CmdPollMS    int
}

func Load() Config {
	port, _ := strconv.ParseUint(getenv("CLP_PORT", "44818"), 10, 16)
	return Config{
		GRPCAddr:     getenv("GRPC_ADDR", ":50051"),
		CLPHost:      getenv("CLP_HOST", "192.168.1.14"),
		CLPPort:      uint16(port),
		LogLevel:     getenv("LOG_LEVEL", "info"),
		QueueDBPath:  getenv("QUEUE_DB_PATH", "./data/queue.db"),
		AckTimeoutMS: getenvInt("CMD_ACK_TIMEOUT_MS", 5000),
		MaxRetries:   getenvInt("CMD_MAX_RETRIES", 3),
		CmdPollMS:    getenvInt("CMD_POLL_MS", 50),
	}
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
