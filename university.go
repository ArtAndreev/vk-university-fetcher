package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

//nolint:funlen
func main() {
	token := flag.String("token", "", "vk access token")
	allCities := flag.Bool("all_cities", false, "all cities") // XXX: about 158000
	workers := flag.Int("workers", runtime.NumCPU(), "parallel fetchers-workers count")

	dbConnStr := flag.String("db_connstr", "postgres@localhost:5432", "db connection string")
	dbName := flag.String("db_name", "postgres", "db table name")

	flag.Parse()

	if *token == "" {
		log.Panicf("vk access token isn't provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := pgxpool.Connect(ctx, "postgresql://"+*dbConnStr+"/"+*dbName)
	if err != nil {
		log.Panicf("failed to connect to database: %s", err)
	}
	defer db.Close()

	vkFetcher := fetcher{
		token: *token,
		client: http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: *workers,
			},
		},
	}

	var (
		wg     sync.WaitGroup
		taskCh = make(chan Entity)
	)

	for i := 0; i < *workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := runWorker(taskCh, db, &vkFetcher); err != nil {
				log.Printf("failed to run worker: %s", err)
			}
		}()
	}

	offset := 0

	for {
		cities, err := vkFetcher.GetRussianCities(context.TODO(), *allCities, offset)
		if err != nil {
			log.Printf("failed to get russian cities, offset %d: %s", offset, err)
			break
		}

		log.Printf("fetched %d cities", len(cities))

		for _, c := range cities {
			taskCh <- c
		}

		if len(cities) != maxCitiesCountPerRequest {
			break
		}

		offset += maxCitiesCountPerRequest
	}

	close(taskCh)

	wg.Wait()

	log.Printf("done")
}

func runWorker(taskCh <-chan Entity, db *pgxpool.Pool, vkFetcher *fetcher) error {
	for city := range taskCh {
		city.Title = strings.TrimSpace(city.Title)

		offset := 0

		for {
			unis, err := vkFetcher.GetUniversities(context.TODO(), city.ID, offset)
			if err != nil {
				return fmt.Errorf("failed to get russian unis, offset %d: %w", offset, err)
			}

			log.Printf("city '%s': fetched %d unis", city.Title, len(unis))

			for i, u := range unis {
				title := strings.TrimSpace(u.Title)

				res, err := db.Exec(
					context.TODO(),
					`INSERT INTO university (city, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
					city.Title, title,
				)
				if err != nil {
					return fmt.Errorf("city '%s': failed to insert uni: %s", city.Title, err)
				}

				if res.RowsAffected() == 0 {
					log.Printf(
						"city '%s': uni exists (id = %d, name = '%s'), skipping: %d/%d",
						city.Title, u.ID, title, i, len(unis),
					)
				}
			}

			if len(unis) != maxUniCountPerRequest {
				break
			}

			offset += maxUniCountPerRequest
		}
	}

	return nil
}
