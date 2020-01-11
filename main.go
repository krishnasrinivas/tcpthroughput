package main

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/minio/cli"
)

func main() {
	app := cli.NewApp()
	app.Usage = "HTTP throughput benchmark"
	app.Commands = []cli.Command{
		{
			Name:   "client",
			Usage:  "run client",
			Action: runClient,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "server",
					Usage: "server:port",
					Value: "",
				},
				cli.IntFlag{
					Name:  "threads",
					Usage: "threads",
					Value: 10,
				},
			},
		},
		{
			Name:   "server",
			Usage:  "run server",
			Action: runServer,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "port",
					Usage: "port",
					Value: "8000",
				},
			},
		},
		{
			Name:   "muxserver",
			Usage:  "run mux server",
			Action: runMuxServer,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "port",
					Usage: "port",
					Value: "8000",
				},
				cli.StringFlag{
					Name:  "servers",
					Usage: "server1:8001,server2:8001,server3:8001",
					Value: "",
				},
			},
		},
	}
	app.RunAndExitOnError()
}

func runMuxServer(ctx *cli.Context) {
	port := ctx.String("port")
	serversStr := ctx.String("servers")
	if serversStr == "" {
		log.Fatal("servers not provided")
	}
	servers := strings.Split(serversStr, ",")

	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle connections in a new goroutine.
		go handleMuxRequest(conn, servers)
	}
}

func handleMuxRequest(connServer net.Conn, servers []string) {
	var connClients []net.Conn
	for _, server := range servers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			log.Fatal(err)
		}
		connClients = append(connClients, conn)
		defer conn.Close()
	}
	buf := make([]byte, 2*1024*1024)
	for {
		for _, connClient := range connClients {
			n, err := connServer.Read(buf)
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println(err)
				continue
			}
			m, err := connClient.Write(buf[:n])
			if err != nil {
				log.Println(err)
				continue
			}
			if n != m {
				log.Println("n != m")
			}
		}
	}
}

func runServer(ctx *cli.Context) {
	port := ctx.String("port")
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	io.Copy(ioutil.Discard, conn)
	conn.Close()
}

func runClient(ctx *cli.Context) {
	server := ctx.String("server")
	threads := ctx.Int("threads")
	b := make([]byte, 1024*0124)
	wg := &sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", server)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			for {
				_, err := conn.Write(b)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}
	time.Sleep(time.Second)
	wg.Wait()
}
