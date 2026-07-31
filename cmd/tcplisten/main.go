// Comando de escuta (server TCP): abre uma porta e fica recebendo os dados que
// o CLP envia. Serve como base isolada — depois esse fluxo pode ser movido para
// um método/pacote e integrado ao resto do projeto.
//
// Uso (PowerShell):
//
//	$env:TCP_LISTEN_ADDR=":9000"; go run ./cmd/tcplisten
//
// Se a variável não for setada, escuta em :9000 por padrão.
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// readTimeout: se o CLP parar de enviar sem fechar o socket (queda de rede,
// cabo), liberamos a conexão em vez de deixar a goroutine presa para sempre.
// Ajuste para o intervalo real de envio do CLP.
const readTimeout = 30 * time.Second

func main() {
	addr := os.Getenv("TCP_LISTEN_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	// captura Ctrl+C / SIGTERM para encerrar limpo
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		log.Fatalf("erro ao abrir porta %s: %v", addr, err)
	}
	defer ln.Close()
	log.Printf("escutando em %s", addr)

	// ao receber o sinal, fecha o listener: o Accept() abaixo retorna
	// net.ErrClosed e saímos do loop.
	go func() {
		<-ctx.Done()
		log.Println("encerrando listener...")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("erro no accept: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("conexão de %s", remote)

	buf := make([]byte, 4096)
	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		n, err := conn.Read(buf)
		if n > 0 {
			data := buf[:n]
			// TCP é stream, não mensagem: um Read pode trazer dados parciais ou
			// vários "pacotes" juntos. Quando soubermos o enquadramento do CLP
			// (delimitador, tamanho fixo ou header com length), trocamos por
			// bufio.Scanner / io.ReadFull. Por ora só logamos o cru.
			log.Printf("[%s] recebido %d bytes: % x", remote, n, data)
			// TODO: parsear/processar os dados do CLP aqui.
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				log.Printf("[%s] conexão fechada pelo CLP", remote)
			case isTimeout(err):
				log.Printf("[%s] timeout de leitura (%s sem dados)", remote, readTimeout)
			default:
				log.Printf("[%s] erro de leitura: %v", remote, err)
			}
			return
		}
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
