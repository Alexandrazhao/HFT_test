package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orderbook_turnover/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run() error {
	srv, err := server.New()
	if err != nil {
		return err
	}

	// Prefer fixed port when provided (works with Vite proxy). Fallback to random.
	addr := "127.0.0.1:0"
	if p := os.Getenv("PORT"); p != "" {
		addr = fmt.Sprintf("127.0.0.1:%s", p)
	} else if p := os.Getenv("APP_PORT"); p != "" { // alternate env name
		addr = fmt.Sprintf("127.0.0.1:%s", p)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	url := fmt.Sprintf("http://%s", ln.Addr().String())
	log.Printf("serving on %s", url)

	// No auto-open browser; keep server headless by default.

	httpServer := &http.Server{
		Handler: loggingMiddleware(srv),
	}

	go func() {
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-sigCh

	log.Printf("received signal: %v; shutting down...", sig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (lw *loggingResponseWriter) WriteHeader(status int) {
	lw.status = status
	lw.ResponseWriter.WriteHeader(status)
}

func (lw *loggingResponseWriter) Write(b []byte) (int, error) {
	if lw.status == 0 {
		lw.status = http.StatusOK
	}
	return lw.ResponseWriter.Write(b)
}

func (lw *loggingResponseWriter) Flush() {
	if fl, ok := lw.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (lw *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := lw.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("hijacker not supported")
}

func (lw *loggingResponseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := lw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(lrw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, lrw.status, time.Since(start).Round(time.Millisecond))
	})
}
