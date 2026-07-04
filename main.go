package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type MCPMessage struct {
	Method string `json:"method,omitempty"`
}

type Proxy struct {
	socketPath       string
	initRequest      string
	initNotification string

	pendingMessages []string
	mu              sync.Mutex
	cond            *sync.Cond

	isFirstMsgAfterReconnect bool
	isReconnecting           bool

	in  io.Reader
	out io.Writer
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <socket_path>\n", os.Args[0])
		os.Exit(1)
	}

	p := &Proxy{
		socketPath: os.Args[1],
		in:         os.Stdin,
		out:        os.Stdout,
	}
	p.cond = sync.NewCond(&p.mu)

	// 1. 標準入力(Stdin)からの読み取りをバックグラウンドで開始
	go p.readStdin()

	// 2. サーバーへの接続と送受信ループを開始
	p.connectToServer()
}

// readStdin は入力(デフォルト標準入力)からメッセージを読み取り、状態のキャッシュや送信キューへの追加を行う
func (p *Proxy) readStdin() {
	scanner := bufio.NewScanner(p.in)
	// MCPのペイロードは大きくなることがあるためバッファを拡張
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// initialize や initialized を検知してキャッシュする
		var msg MCPMessage
		if err := json.Unmarshal([]byte(line), &msg); err == nil {
			p.mu.Lock()
			if msg.Method == "initialize" {
				p.initRequest = line
			} else if msg.Method == "notifications/initialized" {
				p.initNotification = line
			}
			p.mu.Unlock()
		}

		// キューに追加してWriterを起こす
		p.mu.Lock()
		p.pendingMessages = append(p.pendingMessages, line)
		p.cond.Signal()
		p.mu.Unlock()
	}
}

// connectToServer はソケットへの接続と再接続（バックオフ）を管理する
func (p *Proxy) connectToServer() {
	for {
		conn, err := net.Dial("unix", p.socketPath)
		if err != nil {
			p.mu.Lock()
			p.isReconnecting = true
			p.mu.Unlock()
			// 接続に失敗した場合は1秒待機して再試行
			time.Sleep(1 * time.Second)
			continue
		}

		p.mu.Lock()
		if p.isReconnecting {
			p.isFirstMsgAfterReconnect = true
			// 再接続成功時、キャッシュされた初期化メッセージを未送信キューの先頭に積む
			var newPending []string
			if p.initRequest != "" {
				newPending = append(newPending, p.initRequest)
			}
			if p.initNotification != "" {
				newPending = append(newPending, p.initNotification)
			}
			p.pendingMessages = append(newPending, p.pendingMessages...)
		}
		p.isReconnecting = false
		p.mu.Unlock()

		// 接続が確立されている間の処理（切断されるまでブロックする）
		p.handleConnection(conn)
	}
}

// handleConnection は接続ごとの読み書きを行う
func (p *Proxy) handleConnection(conn net.Conn) {
	writeDone := make(chan struct{})
	var connClosed bool

	// Writer Goroutine: キューに溜まったメッセージをサーバーに送信する
	go func() {
		defer close(writeDone)
		for {
			p.mu.Lock()
			for len(p.pendingMessages) == 0 && !connClosed {
				p.cond.Wait()
			}
			if connClosed {
				p.mu.Unlock()
				return
			}

			msg := p.pendingMessages[0]
			p.pendingMessages = p.pendingMessages[1:]
			p.mu.Unlock()

			if _, err := conn.Write([]byte(msg + "\n")); err != nil {
				// 送信失敗時、メッセージをキューの先頭に戻して終了する
				p.mu.Lock()
				p.pendingMessages = append([]string{msg}, p.pendingMessages...)
				connClosed = true
				p.mu.Unlock()
				conn.Close()
				return
			}
		}
	}()

	// Reader Loop: サーバーからのレスポンスを標準出力に流す
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		p.mu.Lock()
		if p.isFirstMsgAfterReconnect {
			p.isFirstMsgAfterReconnect = false
			p.mu.Unlock()
			// 再接続後の最初のレスポンス（initializeへの応答）はエージェントには見せずに破棄する
			continue
		}
		p.mu.Unlock()

		p.out.Write([]byte(line + "\n"))
	}

	// ソケットが切断された（EOF等）場合の終了処理
	conn.Close()

	p.mu.Lock()
	connClosed = true
	p.isReconnecting = true
	p.cond.Signal() // 待機しているWriterを起こして終了させる
	p.mu.Unlock()

	<-writeDone // Writerが安全に終了するのを待つ
}
