package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/polapolo/timescaledbbenchmark/pkg"
)

const numOfUserIDs = 10 // number of users
const numOfOrders = 10  // order matched per user
const numOfTrades = 1   // trade matched per order

const numOfWorkers = 4

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	// connect db
	db := connectDB(ctx)
	defer db.Close()

	// refresh table
	refreshSchema(ctx, db)

	startTime := time.Now()
	concurrentInsertOrders(ctx, db)
	concurrentInsertTrades(ctx, db)
	endTime := time.Since(startTime)

	log.Println("Total Time:", endTime.Milliseconds(), "ms")
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
			offset := numOfOrders * (i - 1)

			// trades
			for k := 1; k <= numOfTrades; k++ {
				queries = append(queries, `INSERT INTO trades(order_id, lot, price, created_at) VALUES (`+strconv.Itoa(j+offset)+`,10,1000,'`+time.Now().Format(time.RFC3339)+`');`)
			}
		}
	}

	return queries
}

func concurrentInsertOrders(ctx context.Context, db *pgxpool.Pool) {
	orderQueries := generateInsertOrderQueries()

	insertWorkerPool := pkg.NewWorkerPool(numOfWorkers)
	insertWorkerPool.Run()

	totalTask := len(orderQueries)
	resultC := make(chan int, totalTask)

	for i := 0; i < totalTask; i++ {
		query := orderQueries[i]
		id := i
		insertWorkerPool.AddTask(func() {
			log.Printf("[main] Starting task %d", id)

			_, err := db.Exec(ctx, query)
			if err != nil {
				log.Fatalln(query, err)
			}

			resultC <- id
		})
	}

	for i := 0; i < totalTask; i++ {
		result := <-resultC
		log.Printf("[FNISH] Task %d", result)
	}
}

func concurrentInsertTrades(ctx context.Context, db *pgxpool.Pool) {
	tradeQueries := generateInsertTradeQueries()

	insertWorkerPool := pkg.NewWorkerPool(numOfWorkers)
	insertWorkerPool.Run()

	totalTask := len(tradeQueries)
	resultC := make(chan int, totalTask)

	for i := 0; i < totalTask; i++ {
		query := tradeQueries[i]
		id := i
		insertWorkerPool.AddTask(func() {
			log.Printf("[main] Starting task %d", id)

			_, err := db.Exec(ctx, query)
			if err != nil {
				log.Fatalln(query, err)
			}

			resultC <- id
		})
	}

	for i := 0; i < totalTask; i++ {
		result := <-resultC
		log.Printf("[FNISH] Task %d", result)
	}
}
