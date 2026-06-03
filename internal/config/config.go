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
}

func Load() Config {
	port, _ := strconv.ParseUint(getenv("CLP_PORT", "44818"), 10, 16)
	return Config{
		GRPCAddr: getenv("GRPC_ADDR", ":50051"),
		CLPHost:  getenv("CLP_HOST", "192.168.1.1"),
		CLPPort:  uint16(port),
		LogLevel: getenv("LOG_LEVEL", "info"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
