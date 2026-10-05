package main

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

const (
	ServerURL  = "wss://YOUR_FOREIGN_SERVER_IP:8443/ws-tunnel" // آدرس سرور خارج
	AuthHeader = "X-Tunnel-Auth"
	AuthSecret = "CHANGE_THIS_SECURE_TOKEN_12345"
	LocalAddr  = "0.0.0.0:1080"                             // پورتی که در ایران باز می‌شود
	TargetAddr = "127.0.0.1:8080"                           // پورتی که در خارج به آن وصل می‌شود
)

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
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

func startClient() {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // در صورت استفاده از IP خودامضا
		},
		ReadBufferSize:  128 * 1024,
		WriteBufferSize: 128 * 1024,
	}

	headers := http.Header{}
	headers.Set(AuthHeader, AuthSecret)

	for {
		log.Println("[Client] Connecting to foreign server...")
		ws, _, err := dialer.Dial(ServerURL, headers)
		if err != nil {
			log.Printf("[Client] Dial error: %v. Retrying in 3s...", err)
			time.Sleep(3 * time.Second)
			continue
		}

		adapter := &websocketConn{Conn: ws}
		yamuxConfig := yamux.DefaultConfig()
		yamuxConfig.EnableKeepAlive = true
		yamuxConfig.KeepAliveInterval = 15 * time.Second

		session, err := yamux.Client(adapter, yamuxConfig)
		if err != nil {
			log.Printf("[Client] Yamux client error: %v", err)
			ws.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("[Client] Tunnel established successfully!")

		for {
			stream, err := session.Accept()
			if err != nil {
				log.Printf("[Client] Session closed: %v", err)
				break
			}

			go func(remoteStream net.Conn) {
				localConn, err := net.Dial("tcp", TargetAddr)
				if err != nil {
					remoteStream.Close()
					return
				}
				setTCPOptimizations(localConn)

				go pipe(remoteStream, localConn)
				go pipe(localConn, remoteStream)
			}(stream)
		}

		session.Close()
		ws.Close()
		time.Sleep(2 * time.Second)
	}
}

func main() {
	startClient()
}
