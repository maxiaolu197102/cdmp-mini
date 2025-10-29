package framework

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Server represents a running instance of the iam-apiserver for testing.
type Server struct {
	binaryPath  string
	baseDir     string
	apiPort     string
	cmd         *exec.Cmd
	stdout      bytes.Buffer
	stderr      bytes.Buffer
	logFile     string
	fallbackDir string
}

// NewServer creates a new server instance for testing.
func NewServer(binaryPath, baseDir, apiPort string) *Server {
	return &Server{
		binaryPath: binaryPath,
		baseDir:    baseDir,
		apiPort:    apiPort,
	}
}

// Start starts the API server.
func (s *Server) Start() error {
	outputDir := filepath.Join(s.baseDir, "logs")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	logFile := filepath.Join(outputDir, "iam-apiserver.log")

	fallbackDir := filepath.Join(s.baseDir, "fallback-logs")
	if err := os.MkdirAll(fallbackDir, 0755); err != nil {
		return fmt.Errorf("failed to create fallback log directory: %w", err)
	}

	args := []string{
		"--server.mode=debug",
		"--server.healthz=true",
		"--server.fast-debug-startup=true",
		"--server.insecure-port=" + s.apiPort,
		"--server.enable-metrics=false",
		"--server.enable-profiling=false",
		"--log.level=debug",
		"--log.output-paths=stdout," + logFile,
		"--feature.enable-audit=true",
		"--audit.log-file=" + filepath.Join(outputDir, "iam-audit.log"),
		"--audit.enable-metrics=false",
		"--producer.fallback-dir=" + fallbackDir,
	}
	s.cmd = exec.Command(s.binaryPath, args...)
	s.cmd.Stdout = &s.stdout
	s.cmd.Stderr = &s.stderr
	s.logFile = logFile
	s.fallbackDir = fallbackDir

	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	return nil
}

// Stop stops the API server.
func (s *Server) Stop() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	if err := s.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill server process: %w", err)
	}
	return s.cmd.Wait()
}

// Stdout returns the stdout buffer of the server process.
func (s *Server) Stdout() string {
	return s.stdout.String()
}

// Stderr returns the stderr buffer of the server process.
func (s *Server) Stderr() string {
	return s.stderr.String()
}

// LogFile returns the path to the server's log file.
func (s *Server) LogFile() string {
	return s.logFile
}

// FallbackDir returns the path to the producer's fallback directory.
func (s *Server) FallbackDir() string {
	return s.fallbackDir
}
