package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

const (
	AuthHeader = "X-Tunnel-Auth"
	AuthSecret = "CHANGE_THIS_SECURE_TOKEN_12345" // حتماً تغییر دهید
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:   128 * 1024,
	WriteBufferSize:  128 * 1024,
	HandshakeTimeout: 10 * time.Second,
	CheckOrigin:      func(r *http.Request) bool { return true },
}

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

type wsConnWrapper struct {
	*websocket.ConnDefaultImpl
	r io.Reader
}

func newWSWrapper(c *websocket.Conn) io.ReadWriteCloser {
	return &wsConnAdapter{conn: c}
}

type wsConnAdapter struct {
	conn *websocket.ConnHelper
	r    io.Reader
}

type websocketConn struct {
	*websocket.Conn
	reader io.Reader
}

func (w *websocketConn) Read(p []byte) (int, error) {
	for {
		if w.reader == nil {
			msgType, r, err := w.Conn.NextReader()
			if err != nil {
				return 0, err
			}
			if msgType != websocket.BinaryMessage && msgType != websocket.TextMessage {
				continue
			}
			w.reader = r
		}
		n, err := w.reader.Read(p)
		if err == io.EOF {
			w.reader = nil
			continue
		}
		return n, err
	}
}

func (w *websocketConn) Write(p []byte) (int, error) {
	err := w.Conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func setTCPOptimizations(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(256 * 1024)
		_ = tcp.SetWriteBuffer(256 * 1024)
	}
}

func pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)

	_, _ = io.CopyBuffer(dst, src, *bufPtr)
}

func handleTunnel(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(AuthHeader) != AuthSecret {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] Upgrade error: %v", err)
		return
	}
	defer ws.Close()

	log.Println("[Server] Client connected via WebSocket. Initializing Yamux...")

	adapter := &websocketConn{Conn: ws}
	yamuxConfig := yamux.DefaultConfig()
	yamuxConfig.EnableKeepAlive = true
	yamuxConfig.KeepAliveInterval = 15 * time.Second

	session, err := yamux.Server(adapter, yamuxConfig)
	if err != nil {
		log.Printf("[Server] Yamux server error: %v", err)
		return
	}
	defer session.Close()

	// لیسنر داخلی روی سرور خارج برای دریافت ترافیک ورود از پروکسی/V2Ray (مثلا پورت 8080)
	localListener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		log.Printf("[Server] Failed to listen on local port 8080: %v", err)
		return
	}
	defer localListener.Close()

	log.Println("[Server] Listening on 127.0.0.1:8080 for incoming target traffic...")

	for {
		localConn, err := localListener.Accept()
		if err != nil {
			break
		}
		setTCPOptimizations(localConn)

		stream, err := session.Open()
		if err != nil {
			localConn.Close()
			log.Printf("[Server] Yamux stream open error: %v", err)
			break
		}

		go pipe(stream, localConn)
		go pipe(localConn, stream)
	}
}

func main() {
	wsPath := "/ws-tunnel"
	listenAddr := ":8443"

	http.HandleFunc(wsPath, handleTunnel)

	server := &http.Server{
		Addr: listenAddr,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
		},
	}

	log.Printf("[Server] Production Tunnel Server listening on https://0.0.0.0%s%s\n", listenAddr, wsPath)
	// در صورت داشتن گواهی TLS واقعی:
	// err := server.ListenAndServeTLS("server.crt", "server.key")
	err := server.ListenAndServe() // اگر پشت Nginx/Caddy معکوس پروکسی شده است
	if err != nil {
		log.Fatalf("Server Listen error: %v", err)
	}
}
