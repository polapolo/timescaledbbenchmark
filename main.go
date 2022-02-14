package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
)

const numOfWorkers = 4

const numOfUserIDs = 1          // scale
const numOfInsertQueries = 1000 // queries

var stockCodes = []string{
	"BBCA",
	"BBRI",
	"BBNI",
	"BBYB",
	"BBHI",
	"AMAR",
	"ARTO",
}

var orderType = []string{
	"B",
	"S",
}

var price = []int{
	1000,
	2000,
}

func main() {
	ctx := context.Background()

	db := connectDB(ctx)
	defer db.Close()

	queries := generateInsertQueries()

	wg := sync.WaitGroup{}
	for i := 0; i < len(queries); i++ {
		wg.Add(1)
		go insert(ctx, wg, db, queries[i])
	}
	wg.Wait()

	log.Fatalf("%+v", queries)
	// refreshSchema(ctx, db)
}

func connectDB(ctx context.Context) *pgxpool.Pool {
	connStr := "postgres://postgres:password@localhost:5432/benchmark"
	db, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return db
}

func refreshSchema(ctx context.Context, db *pgxpool.Pool) {
	_, err := db.Exec(ctx, `DROP TABLE IF EXISTS orders`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id BIGSERIAL PRIMARY KEY,
		user_id bigint,
		stock_code varchar(6),
		type VARCHAR(1),
		lot bigint,
		price int,
		status int,
		created_at TIMESTAMPTZ
	)`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `SELECT create_hypertable('orders', 'timestamp')`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `DROP TABLE IF EXISTS initial_cash`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS initial_cash (
		id BIGSERIAL PRIMARY KEY,
		user_id bigint,
		stock_code varchar(6),
		type VARCHAR(1),
		lot bigint,
		price int,
		status int
	)`)
	if err != nil {
		log.Fatalln(err)
	}
}

func generateInsertQueries() []string {
	queries := make([]string, numOfUserIDs*numOfInsertQueries)
	queryIndex := 0
	for i := 1; i <= numOfUserIDs; i++ {
		for j := 0; j < numOfInsertQueries; j++ {
			orderType := "B"
			if j%2 == 1 {
				orderType = "S"
			}
			queries[queryIndex] = `
			INSERT INTO orders(user_id, stock_code, type, lot, price, status) VALUES (` + strconv.Itoa(i) + `,"` + "BBCA" + `","` + orderType + `",` + strconv.Itoa(10) + `,` + strconv.Itoa(price[rand.Intn(2)]) + `,1);`
			queryIndex++
		}
	}

	return queries
}

func insert(ctx context.Context, wg sync.WaitGroup, db *pgxpool.Pool, query string) {
	defer wg.Done()

	_, err := db.Exec(ctx, query)
	if err != nil {
		log.Fatalln(query, err)
	}
}

// func updateCPUTagID() {
// 	queryCreateTable := `CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));`
// 	_, err = dbpool.Exec(ctx, queryCreateTable)
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Unable to create SENSORS table: %v\n", err)
// 		os.Exit(1)
// 	}
// }
