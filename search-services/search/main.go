package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	searchpb "github.com/Lullabalu/microservice-search-system/proto/search"
	"github.com/Lullabalu/microservice-search-system/search/adapters/db"
	searchgrpc "github.com/Lullabalu/microservice-search-system/search/adapters/grpc"
	"github.com/Lullabalu/microservice-search-system/search/adapters/words"
	"github.com/Lullabalu/microservice-search-system/search/config"
	"github.com/Lullabalu/microservice-search-system/search/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "server configuration")
	flag.Parse()

	cfg := config.MustLoad(configPath)

	if err := run(&cfg); err != nil {
		log.Fatalf("Сервер не запустился")
		os.Exit(1)
	}
}

func Updater(s core.Searcher, cfg *config.Config) {
	err := s.UpdateIndex()
	if err != nil {
		log.Println("Не удалось обновить индекс", err)
	}

	for range time.Tick(cfg.IndexTTL) {
		if err = s.UpdateIndex(); err != nil {
			log.Println("Не удалось обновить индекс", err)
		}
	}
}

func run(cfg *config.Config) error {

	storage, err := db.New(cfg.DBAddress)
	if err != nil {
		return err
	}

	words, err := words.NewClient(cfg.WordsAddress)

	if err != nil {
		return err
	}

	searcher := core.NewService(storage, words)
	listener, err := net.Listen("tcp", cfg.Address)

	if err != nil {
		return err
	}

	go Updater(searcher, cfg)

	s := grpc.NewServer()
	searchpb.RegisterSearchServer(s, searchgrpc.NewServer(searcher))
	reflection.Register(s)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
