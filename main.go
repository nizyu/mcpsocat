package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MCPMessage struct {
	ID      string `json:"id,omitempty"`
	Method  string `json:"method,omitempty"`
	Params  any    `json:"params,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

type Proxy struct {
	in               *os.File
	out              *os.File
	socketPath       string
	pendingMessages  []string
	mu               sync.Mutex
	cond             *sync.Cond
	isReconnecting   bool
	cachedInitReq    string
	cachedInitNotif  string
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: mcpsocat <socket-path>")
	}

	p := &Proxy{
		in:         os.Stdin,
		out:        os.Stdout,
		socketPath: os.Args[1],
	}
	p.cond = sync.NewCond(&p.mu)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go p.readStdin()

	p.connectToServer(ctx)
}

func (p *Proxy) readStdin() {
	scanner := bufio.NewScanner(p.in)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var msg MCPMessage
		if err := json.Unmarshal([]byte(line), &msg); err == nil {
			p.mu.Lock()
			if msg.Method == "initialize" {
				p.cachedInitReq = line
			} else if msg.Method == "notifications/initialized" {
				p.cachedInitNotif = line
			}
			p.mu.Unlock()
		}

		p.mu.Lock()
		p.pendingMessages = append(p.pendingMessages, line)
		p.cond.Signal()
		p.mu.Unlock()
	}
}

func (p *Proxy) connectToServer(ctx context.Context) {
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	for {
		conn, err := net.Dial("unix", p.socketPath)
		if err != nil {
			p.mu.Lock()
						p.isReconnecting = true
						p.mu.Unlock()
			log.Printf("connection failed, retrying in %v...", backoff)
			select {
			case <-ctx.Done():
				log.Println("shutting down...")
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = 1 * time.Second
		log.Printf("connected to %s", p.socketPath)

		reader := bufio.NewReaderSize(conn, 10*1024*1024)

		var initReq, initNotif string
		var reconnecting bool

		p.mu.Lock()
		if p.cachedInitReq != "" || p.cachedInitNotif != "" {
			initReq = p.cachedInitReq
			initNotif = p.cachedInitNotif
			reconnecting = true
		}
		p.mu.Unlock()

		if reconnecting {
			log.Println("replaying init handshake...")
			if err := p.replayInitHandshake(conn, reader, initReq, initNotif); err != nil {
				log.Printf("handshake failed: %v, retrying...", err)
				conn.Close()
				p.mu.Lock()
							p.isReconnecting = true
							p.mu.Unlock()
				log.Printf("handshake failed, retrying in %v...", backoff)
				select {
				case <-ctx.Done():
					log.Println("shutting down...")
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
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
			log.Println("session recovered successfully")
		}

		p.handleConnection(conn, reader)
	}
}

func (p *Proxy) replayInitHandshake(conn net.Conn, reader *bufio.Reader, initReq, initNotif string) error {
	if initReq != "" {
		if _, err := conn.Write([]byte(initReq + "\n")); err != nil {
			return err
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return err
		}
	}
	if initNotif != "" {
		if _, err := conn.Write([]byte(initNotif + "\n")); err != nil {
			return err
		}
	}
	return nil
}

func (p *Proxy) handleConnection(conn net.Conn, reader *bufio.Reader) {
	writeDone := make(chan struct{})
	var connClosed bool

	go func() {
		defer close(writeDone)
		for {
			p.mu.Lock()
			for len(p.pendingMessages) == 0 && !connClosed {
				p.cond.Wait()
			}
			if len(p.pendingMessages) == 0 {
				p.mu.Unlock()
				return
			}
			msg := p.pendingMessages[0]
			p.pendingMessages = p.pendingMessages[1:]
			p.mu.Unlock()

			if _, err := conn.Write([]byte(msg + "\n")); err != nil {
				p.mu.Lock()
					p.pendingMessages = append([]string{msg}, p.pendingMessages...)
					connClosed = true
					p.mu.Unlock()
				conn.Close()
				return
			}
		}
	}()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		p.out.Write([]byte(line + "\n"))
	}

	conn.Close()
	log.Println("connection lost, reconnecting...")

	p.mu.Lock()
	connClosed = true
	p.isReconnecting = true
	p.cond.Signal()
	p.mu.Unlock()

	<-writeDone
}
