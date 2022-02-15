package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
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

	batchInsertOrder(ctx, db)
	batchInsertTrade(ctx, db)
	batchInsertInitialCash(ctx, db)

	// batchUpdateOrder(ctx, db)
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

func generateUpdateOrderQueries() []string {
	queries := make([]string, 0)
	// users
	for i := 1; i <= numOfUserIDs; i++ {
		// orders
		for j := 1; j <= numOfOrders; j++ {
			offset := numOfOrders * (i - 1)

			query := `UPDATE orders SET status = 2 WHERE id = ` + strconv.Itoa(j+offset)
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

func generateInsertInitialCashQueries() []string {
	queries := make([]string, 0)
	// users
	for i := 1; i <= numOfUserIDs; i++ {
		queries = append(queries, `INSERT INTO initial_cash(user_id, cash_on_hand) VALUES (`+strconv.Itoa(i)+`,1000);`)
	}

	return queries
}

type result struct {
	WorkerID  int
	SpeedInMs int64
}

func concurrentInsertOrders(ctx context.Context, db *pgxpool.Pool) {
	orderQueries := generateInsertOrderQueries()

	startTime := time.Now()

	insertWorkerPool := pkg.NewWorkerPool(numOfWorkers)
	insertWorkerPool.Run()

	totalTask := len(orderQueries)
	resultC := make(chan result, totalTask)

	for i := 0; i < totalTask; i++ {
		query := orderQueries[i]
		id := i
		insertWorkerPool.AddTask(func() {
			// log.Printf("[main] Starting task %d", id)

			startTime := time.Now()

			_, err := db.Exec(ctx, query)
			if err != nil {
				log.Fatalln(query, err)
			}

			timeElapsed := time.Since(startTime)

			resultC <- result{
				WorkerID:  id,
				SpeedInMs: timeElapsed.Microseconds(),
			}
		})
	}

	var totalSpeedInMs int64
	for i := 0; i < totalTask; i++ {
		result := <-resultC
		totalSpeedInMs += result.SpeedInMs
		// log.Printf("[FNISH] Task %d", result)
	}

	avgSpeedInMs := float64(float64(totalSpeedInMs) / float64(totalTask))
	totalRecord := numOfUserIDs * numOfOrders * numOfTrades
	avgSpeedInSecond := float64(float64(avgSpeedInMs) / float64(1000000))
	recordPerSecond := float64(1) / float64(avgSpeedInSecond) * float64(totalRecord)
	log.Println("Concurrent Insert Order Speed:", avgSpeedInMs, "microsecond | ", int64(recordPerSecond), "records/s")

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Insert Order Speed:", timeElapsed.Milliseconds(), "ms")
}

func concurrentUpdateOrders(ctx context.Context, db *pgxpool.Pool) {
	orderQueries := generateUpdateOrderQueries()

	startTime := time.Now()

	workerPool := pkg.NewWorkerPool(numOfWorkers)
	workerPool.Run()

	totalTask := len(orderQueries)
	resultC := make(chan result, totalTask)

	for i := 0; i < totalTask; i++ {
		query := orderQueries[i]
		id := i
		workerPool.AddTask(func() {
			// log.Printf("[main] Starting task %d", id)

			startTime := time.Now()

			_, err := db.Exec(ctx, query)
			if err != nil {
				log.Fatalln(query, err)
			}

			timeElapsed := time.Since(startTime)

			resultC <- result{
				WorkerID:  id,
				SpeedInMs: timeElapsed.Microseconds(),
			}
		})
	}

	var totalSpeedInMs int64
	for i := 0; i < totalTask; i++ {
		result := <-resultC
		totalSpeedInMs += result.SpeedInMs
		// log.Printf("[FNISH] Task %d", result)
	}

	avgSpeedInMs := float64(float64(totalSpeedInMs) / float64(totalTask))
	totalRecord := numOfUserIDs * numOfOrders * numOfTrades
	avgSpeedInSecond := float64(float64(avgSpeedInMs) / float64(1000000))
	recordPerSecond := float64(1) / float64(avgSpeedInSecond) * float64(totalRecord)
	log.Println("Concurrent Update Order Speed:", avgSpeedInMs, "microsecond | ", int64(recordPerSecond), "records/s")

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Update Order Speed:", timeElapsed.Milliseconds(), "ms")
}

func concurrentInsertTrades(ctx context.Context, db *pgxpool.Pool) {
	tradeQueries := generateInsertTradeQueries()

	startTime := time.Now()

	insertWorkerPool := pkg.NewWorkerPool(numOfWorkers)
	insertWorkerPool.Run()

	totalTask := len(tradeQueries)
	resultC := make(chan result, totalTask)

	for i := 0; i < totalTask; i++ {
		query := tradeQueries[i]
		id := i
		insertWorkerPool.AddTask(func() {
			// log.Printf("[main] Starting task %d", id)

			startTime := time.Now()

			_, err := db.Exec(ctx, query)
			if err != nil {
				log.Fatalln(query, err)
			}

			timeElapsed := time.Since(startTime)

			resultC <- result{
				WorkerID:  id,
				SpeedInMs: timeElapsed.Microseconds(),
			}
		})
	}

	var totalSpeedInMs int64
	for i := 0; i < totalTask; i++ {
		result := <-resultC
		totalSpeedInMs += result.SpeedInMs
		// log.Printf("[FNISH] Task %d", result)
	}

	avgSpeedInMs := float64(float64(totalSpeedInMs) / float64(totalTask))
	totalRecord := numOfUserIDs * numOfOrders * numOfTrades
	avgSpeedInSecond := float64(float64(avgSpeedInMs) / float64(1000000))
	recordPerSecond := float64(1) / float64(avgSpeedInSecond) * float64(totalRecord)
	log.Println("Concurrent Insert Trade Speed:", avgSpeedInMs, "microsecond | ", int64(recordPerSecond), "records/s")

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Insert Trade Speed:", timeElapsed.Milliseconds(), "ms")
}

// https://github.com/jackc/pgx/issues/374
func batchInsertOrder(ctx context.Context, db *pgxpool.Pool) {
	startTime := time.Now()

	orderQueries := generateInsertOrderQueries()

	// orderQueriesChunks := chunkSlice(orderQueries, 5000)

	// for _, orderQueriesChunk := range orderQueriesChunks {
	batch := &pgx.Batch{}
	// load insert statements into batch queue
	for i := range orderQueries {
		batch.Queue(orderQueries[i])
	}
	batch.Queue("select count(*) from orders")

	// send batch to connection pool
	br := db.SendBatch(ctx, batch)
	defer br.Close()
	// execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}

	// //Compare length of results slice to size of table
	// fmt.Printf("size of results: %d\n", len(orderQueries))
	// //check size of table for number of rows inserted
	// // result of last SELECT statement
	// var rowsInserted int
	// br.QueryRow().Scan(&rowsInserted)
	// fmt.Printf("size of table: %d\n", rowsInserted)
	// }

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Batch Insert Order Speed:", timeElapsed.Milliseconds(), "ms")
}

func batchInsertTrade(ctx context.Context, db *pgxpool.Pool) {
	startTime := time.Now()

	queries := generateInsertTradeQueries()

	// queriesChunks := chunkSlice(queries, 5000)

	// for _, queriesChunk := range queriesChunks {
	batch := &pgx.Batch{}
	// load insert statements into batch queue
	for i := range queries {
		batch.Queue(queries[i])
	}
	batch.Queue("select count(*) from trades")

	// send batch to connection pool
	br := db.SendBatch(ctx, batch)
	defer br.Close()
	// execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}

	// //Compare length of results slice to size of table
	// fmt.Printf("size of results: %d\n", len(queries))
	// //check size of table for number of rows inserted
	// // result of last SELECT statement
	// var rowsInserted int
	// br.QueryRow().Scan(&rowsInserted)
	// fmt.Printf("size of table: %d\n", rowsInserted)
	// }

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Batch Insert Trade Speed:", timeElapsed.Milliseconds(), "ms")
}

func batchInsertInitialCash(ctx context.Context, db *pgxpool.Pool) {
	startTime := time.Now()

	queries := generateInsertInitialCashQueries()

	// queriesChunks := chunkSlice(queries, 5000)

	// for _, queriesChunk := range queriesChunks {
	batch := &pgx.Batch{}
	// load insert statements into batch queue
	for i := range queries {
		batch.Queue(queries[i])
	}
	batch.Queue("select count(*) from initial_cash")

	// send batch to connection pool
	br := db.SendBatch(ctx, batch)
	defer br.Close()
	// execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}

	// //Compare length of results slice to size of table
	// fmt.Printf("size of results: %d\n", len(queries))
	// //check size of table for number of rows inserted
	// // result of last SELECT statement
	// var rowsInserted int
	// br.QueryRow().Scan(&rowsInserted)
	// fmt.Printf("size of table: %d\n", rowsInserted)
	// }

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Batch Insert Initial Cash Speed:", timeElapsed.Milliseconds(), "ms")
}

// https://github.com/jackc/pgx/issues/374
func batchUpdateOrder(ctx context.Context, db *pgxpool.Pool) {
	startTime := time.Now()

	orderQueries := generateUpdateOrderQueries()

	// orderQueriesChunks := chunkSlice(orderQueries, 10000)

	// for _, orderQueriesChunk := range orderQueriesChunks {
	batch := &pgx.Batch{}
	// load update statements into batch queue
	for i := range orderQueries {
		batch.Queue(orderQueries[i])
	}
	batch.Queue("select count(*) from orders")

	// send batch to connection pool
	br := db.SendBatch(ctx, batch)
	defer br.Close()
	// execute statements in batch queue
	_, err := br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}
	// }

	timeElapsed := time.Since(startTime)
	log.Println("Total Time Batch Update Order Speed:", timeElapsed.Milliseconds(), "ms")
}

func chunkSlice(slice []string, chunkSize int) [][]string {
	var chunkedSlice [][]string

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		if end > len(slice) {
			end = len(slice)
		}

		chunkedSlice = append(chunkedSlice, slice[i:end])
	}

	return chunkedSlice
}
