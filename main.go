package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/polapolo/timescaledbbenchmark/pkg"
)

const numOfUserIDs = 100000 // scale
const numOfOrders = 10      // matched per user
const numOfTrades = 1       // matched per order

const numOfInsertWorkers = 4

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	// connect db
	db := connectDB(ctx)
	defer db.Close()

	// refresh table
	refreshSchema(ctx, db)

	orderQueries := generateInsertOrderQueries()

	insertWorkerPool := pkg.NewWorkerPool(numOfInsertWorkers)
	insertWorkerPool.Run()

	totalTask := len(orderQueries)
	resultC := make(chan bool, totalTask)

	for i := 0; i < totalTask; i++ {
		query := orderQueries[i]
		id := i
		insertWorkerPool.AddTask(func() {
			log.Printf("[main] Starting task %d", id)
			insertWithChannel(ctx, db, query, i)
			resultC <- true
		})
	}

	for i := 0; i < totalTask; i++ {
		result := <-resultC
		log.Println("[FNISH] Task", result)
	}

	// wg := sync.WaitGroup{}

	// // insert order concurrently
	// orderQueries := generateInsertOrderQueries()
	// wg.Add(numOfInsertWorkers)
	// for i := 0; i < numOfInsertWorkers; i++ {
	// 	go insert(ctx, wg, db, orderQueries[i], i)
	// }
	// wg.Wait()

	// // insert trade concurrently
	// tradeQueries := generateInsertTradeQueries()
	// wg.Add(len(tradeQueries))
	// for i := 0; i < len(tradeQueries); i++ {
	// 	go insert(ctx, wg, db, tradeQueries[i], i)
	// }
	// wg.Wait()

	log.Println("DONE")
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
	_, err := db.Exec(ctx, `DROP TABLE IF EXISTS trades`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `DROP TABLE IF EXISTS orders`)
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
		status int
	)`)
	if err != nil {
		log.Fatalln(err)
	}

	// _, err = db.Exec(ctx, `SELECT create_hypertable('orders', 'timestamp')`)
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS trades (
		order_id bigint,
		lot bigint,
		price int,
		created_at TIMESTAMPTZ,
		FOREIGN KEY (order_id) REFERENCES orders (id)
	)`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec(ctx, `SELECT create_hypertable('trades', 'created_at')`)
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
		cash_on_hand bigint
	)`)
	if err != nil {
		log.Fatalln(err)
	}
}

func generateInsertOrderQueries() []string {
	queries := make([]string, 0)
	// users
	for i := 1; i <= numOfUserIDs; i++ {
		// orders
		for j := 1; j <= numOfOrders; j++ {
			orderType := "B"
			if j%2 == 0 {
				orderType = "S"
			}

			offset := numOfOrders * (i - 1)

			query := `INSERT INTO orders(id, user_id, stock_code, type, lot, price, status) VALUES (` + strconv.Itoa(j+offset) + `,` + strconv.Itoa(i) + `,'BBCA','` + orderType + `',10,1000,1);`
			// log.Println(query)
			queries = append(queries, query)
		}
	}

	return queries
}

func generateInsertTradeQueries() []string {
	queries := make([]string, 0)
	// users
	for i := 1; i <= numOfUserIDs; i++ {
		// orders
		for j := 1; j <= numOfOrders; j++ {
			offset := numOfOrders * i

			// trades
			for k := 1; k <= numOfTrades; k++ {
				queries = append(queries, `INSERT INTO trades(order_id, lot, price, created_at) VALUES (`+strconv.Itoa(j+offset)+`,10,1000,'`+time.Now().Format(time.RFC3339)+`');`)
			}
		}
	}

	return queries
}

func insert(ctx context.Context, wg sync.WaitGroup, db *pgxpool.Pool, query string, i int) {
	defer wg.Done()

	_, err := db.Exec(ctx, query)
	if err != nil {
		log.Fatalln(query, err)
	}

	// log.Println(i, query)
}

func insertWithChannel(ctx context.Context, db *pgxpool.Pool, query string, i int) {
	_, err := db.Exec(ctx, query)
	if err != nil {
		log.Fatalln(query, err)
	}

	// log.Println(i, query)
}

// func updateCPUTagID() {
// 	queryCreateTable := `CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));`
// 	_, err = dbpool.Exec(ctx, queryCreateTable)
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Unable to create SENSORS table: %v\n", err)
// 		os.Exit(1)
// 	}
// }
