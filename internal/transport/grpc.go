package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/likith1014/distributed-cache/internal/cache"
	"github.com/likith1014/distributed-cache/pkg/metrics"
)

// Server handles both gRPC and HTTP (metrics/admin) traffic.
type Server struct {
	engine     *cache.Engine
	grpcServer *grpc.Server
	httpServer *http.Server
	metrics    *metrics.CacheMetrics
	logger     *zap.Logger
	grpcPort   int
	httpPort   int
}

// NewServer creates the transport layer.
func NewServer(
	engine *cache.Engine,
	m *metrics.CacheMetrics,
	logger *zap.Logger,
	grpcPort, httpPort int,
) *Server {
	grpcSrv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(16*1024*1024), // 16MB max message
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", healthHandler(engine))
	mux.HandleFunc("/stats", statsHandler(engine))
	mux.HandleFunc("/admin/flush", flushHandler(engine))

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", httpPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		engine:     engine,
		grpcServer: grpcSrv,
		httpServer: httpSrv,
		metrics:    m,
		logger:     logger,
		grpcPort:   grpcPort,
		httpPort:   httpPort,
	}
}

// Start begins serving gRPC and HTTP concurrently.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 2)

	// gRPC server
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
		if err != nil {
			errCh <- fmt.Errorf("grpc listen: %w", err)
			return
		}
		s.logger.Info("gRPC server listening", zap.Int("port", s.grpcPort))
		errCh <- s.grpcServer.Serve(lis)
	}()

	// HTTP server (metrics + admin)
	go func() {
		s.logger.Info("HTTP server listening", zap.Int("port", s.httpPort))
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http serve: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		return s.Shutdown()
	case err := <-errCh:
		return err
	}
}

// Shutdown gracefully stops both servers.
func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.grpcServer.GracefulStop()
	return s.httpServer.Shutdown(ctx)
}

func healthHandler(e *cache.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","ts":"%s"}`, time.Now().UTC().Format(time.RFC3339))
	}
}

func statsHandler(e *cache.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := e.Stats()
		fmt.Fprintf(w, `{"policy":"%v","total_ops":%v,"size":%v}`,
			stats["policy"], stats["total_ops"], stats["size"])
	}
}

func flushHandler(e *cache.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.Flush()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"flushed"}`)
	}
}
