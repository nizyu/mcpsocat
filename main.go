package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	quiet := flag.Bool("q", false, "suppress log output")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-q] <socket_path>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	log.SetPrefix("[mcpsocat] ")
	log.SetFlags(log.Ltime)
	if *quiet {
		log.SetOutput(io.Discard)
	}

	p := &Proxy{
		socketPath: flag.Arg(0),
		in:         os.Stdin,
		out:        os.Stdout,
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
				p.initRequest = line
			} else if msg.Method == "notifications/initialized" {
				p.initNotification = line
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
		select {
		case <-ctx.Done():
			log.Println("shutting down...")
			return
		default:
		}

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

		p.mu.Lock()
		reconnecting := p.isReconnecting
		initReq := p.initRequest
		initNotif := p.initNotification
		p.isReconnecting = false
		p.mu.Unlock()

		reader := bufio.NewReaderSize(conn, 10*1024*1024)

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
			if connClosed {
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
		line = strings.TrimRight(line, "\n\r")
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
