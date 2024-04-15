package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

type connection struct {
	db *sql.DB
}

type ConnectionPool struct {
	m           *sync.Mutex
	channel     chan interface{}
	connections []*connection
	capacity    int
}

func NewConnectionPool(capacity int) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		m:           &sync.Mutex{},
		connections: make([]*connection, 0, capacity),
		channel:     make(chan interface{}, capacity),
		capacity:    capacity,
	}

	for i := 0; i < capacity; i++ {
		pool.connections = append(pool.connections, &connection{newConn()})
		pool.channel <- nil
	}
	return pool, nil
}

func (pool *ConnectionPool) Close() {
	close(pool.channel)
	for i := 0; i < pool.capacity; i++ {
		pool.connections[i].db.Close()
	}
}

func (pool *ConnectionPool) Get() (*connection, error) {
	<-pool.channel // Wait to acquire a token indicating a free connection slot
	pool.m.Lock()
	if len(pool.connections) == 0 {
		pool.m.Unlock()
		return nil, fmt.Errorf("no connections available")
	}
	c := pool.connections[0]
	pool.connections = pool.connections[1:]
	pool.m.Unlock()
	return c, nil
}

func (pool *ConnectionPool) Put(c *connection) {
	pool.m.Lock()
	pool.connections = append(pool.connections, c)
	pool.m.Unlock()

	pool.channel <- nil
}

func simulateConnectionPool() {
	startTime := time.Now()
	pool, _ := NewConnectionPool(10)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := pool.Get()
			if err != nil {
				panic(err)
			}
			_, err = conn.db.Exec("SELECT SLEEP(0.01)")
			if err != nil {
				panic(err)
			}
			pool.Put(conn)
		}()
	}

	wg.Wait()
	pool.Close()
	fmt.Println("All goroutines have completed their DB operations.")
	fmt.Printf("Total execution time: %v\n", time.Since(startTime))
}

func simulateConcurrentRequests() {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < 500; i++ { // Adjust the number based on your database's capability
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db := newConn()
			_, err := db.Exec("SELECT SLEEP(0.01)")
			if err != nil {
				panic(err)
			}
			db.Close()
		}(i)
	}

	wg.Wait()
	fmt.Println("All goroutines have completed their DB operations.")
	fmt.Printf("Total execution time: %v\n", time.Since(startTime))
}

func newConn() *sql.DB {
	connectionString := "myuser:mypassword@tcp(localhost:3306)/mydb?parseTime=true"
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		panic(err)
	}
	return db
}

func main() {
	//simulateConcurrentRequests()
	simulateConnectionPool()
}
