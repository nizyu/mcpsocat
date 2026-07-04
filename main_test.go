package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProxySessionRecovery(t *testing.T) {
	// 1. テスト用のUNIXソケットパスを作成
	tmpDir, err := os.MkdirTemp("", "mcpsocat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "test.sock")

	// 2. ダミーの標準入出力をPipeで作成
	inReader, inWriter := io.Pipe()
	outWriter := io.Discard // テストでは出力を厳密に検証しないので読み捨てでOK

	p := &Proxy{
		socketPath: socketPath,
		in:         inReader,
		out:        outWriter,
	}
	p.cond = sync.NewCond(&p.mu)

	// プロキシをバックグラウンドで起動
	go p.readStdin()
	go p.connectToServer()

	// 3. ダミーのサーバー1を立ち上げる
	l1, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	// サーバー側の接続受け入れをバックグラウンドで待機
	serverConnCh := make(chan net.Conn, 1)
	go func() {
		conn, err := l1.Accept()
		if err == nil {
			serverConnCh <- conn
		}
	}()

	// 少し待ってからプロキシに initialize を流し込む
	time.Sleep(100 * time.Millisecond)
	initMsg := `{"method": "initialize"}`
	inWriter.Write([]byte(initMsg + "\n"))

	// サーバー1で接続とメッセージを確認
	select {
	case conn := <-serverConnCh:
		scanner := bufio.NewScanner(conn)
		if scanner.Scan() {
			msg := scanner.Text()
			if !strings.Contains(msg, "initialize") {
				t.Fatalf("Expected initialize message, got: %s", msg)
			}
			t.Log("Server 1 received initialize successfully.")
		}
		// サーバー1を意図的に落とす（ソケットを閉じる）
		conn.Close()
		l1.Close()
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for connection to server 1")
	}

	// 4. サーバーが落ちている間に通常メッセージを送る（バッファリングされるはず）
	time.Sleep(200 * time.Millisecond)
	normalMsg := `{"method": "test/normal"}`
	inWriter.Write([]byte(normalMsg + "\n"))

	// 5. サーバー2（再起動）を同じパスで立ち上げる
	os.Remove(socketPath) // 念のため古いソケットファイルを削除
	l2, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Failed to listen server 2: %v", err)
	}
	defer l2.Close()

	// サーバー2で再接続を受け入れ
	conn2, err := l2.Accept()
	if err != nil {
		t.Fatalf("Failed to accept server 2: %v", err)
	}
	defer conn2.Close()

	scanner2 := bufio.NewScanner(conn2)

	// 最初に来るメッセージは、キャッシュされた initialize の再送のはず
	if scanner2.Scan() {
		msg := scanner2.Text()
		if !strings.Contains(msg, "initialize") {
			t.Fatalf("Expected re-sent initialize message, got: %s", msg)
		}
		t.Log("Server 2 received re-sent initialize successfully.")
	}

	// 次に来るメッセージは、切断中にバッファリングされていた normalMsg のはず
	if scanner2.Scan() {
		msg := scanner2.Text()
		if !strings.Contains(msg, "test/normal") {
			t.Fatalf("Expected buffered normal message, got: %s", msg)
		}
		t.Log("Server 2 received buffered normal message successfully.")
	}
}
