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

	isReconnecting bool

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

// connectToServer はソケットへの接続と再接続（指数バックオフ）を管理する
func (p *Proxy) connectToServer() {
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	for {
		conn, err := net.Dial("unix", p.socketPath)
		if err != nil {
			p.mu.Lock()
			p.isReconnecting = true
			p.mu.Unlock()
			time.Sleep(backoff)
			// 指数バックオフ: 1s → 2s → 4s → ... → max 30s
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// 接続成功時はバックオフをリセット
		backoff = 1 * time.Second

		p.mu.Lock()
		reconnecting := p.isReconnecting
		initReq := p.initRequest
		initNotif := p.initNotification
		p.isReconnecting = false
		p.mu.Unlock()

		reader := bufio.NewReaderSize(conn, 10*1024*1024)

		// 再接続時はMCPの初期化ハンドシェイクを再実行する
		if reconnecting {
			if err := p.replayInitHandshake(conn, reader, initReq, initNotif); err != nil {
				conn.Close()
				p.mu.Lock()
				p.isReconnecting = true
				p.mu.Unlock()
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			// ハンドシェイクで再送済みの初期化メッセージをキューから除去する
			p.mu.Lock()
			filtered := p.pendingMessages[:0]
			for _, m := range p.pendingMessages {
				var parsed MCPMessage
				if err := json.Unmarshal([]byte(m), &parsed); err == nil {
					if parsed.Method == "initialize" || parsed.Method == "notifications/initialized" {
						continue
					}
				}
				filtered = append(filtered, m)
			}
			p.pendingMessages = filtered
			p.mu.Unlock()
		}

		// 接続が確立されている間の処理（切断されるまでブロックする）
		p.handleConnection(conn, reader)
	}
}

// replayInitHandshake は再接続時にMCPの初期化ハンドシェイクを順序通りに再実行する
// initialize送信 → レスポンス受信（破棄） → initialized送信 の順序を保証する
func (p *Proxy) replayInitHandshake(conn net.Conn, reader *bufio.Reader, initReq, initNotif string) error {
	if initReq != "" {
		// 1. キャッシュされた initialize リクエストを送信
		if _, err := conn.Write([]byte(initReq + "\n")); err != nil {
			return err
		}
		// 2. サーバーからの initialize レスポンスを読み取り、破棄する
		//    （クライアントは既に初期化済みと認識しているため）
		if _, err := reader.ReadString('\n'); err != nil {
			return err
		}
	}
	if initNotif != "" {
		// 3. initialized 通知を送信（レスポンス受信後なので順序が保証される）
		if _, err := conn.Write([]byte(initNotif + "\n")); err != nil {
			return err
		}
	}
	return nil
}

// handleConnection は接続ごとの読み書きを行う
func (p *Proxy) handleConnection(conn net.Conn, reader *bufio.Reader) {
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
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\n\r")
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
