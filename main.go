package main

import (
	"database/sql/driver"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"database/sql"
)

// Model
type Todo struct {
	Id        int       `json:"id"`
	Name      string    `json:"name"`
	Completed bool      `json:"completed"`
	Due       time.Time `json:"due"`
}

// create table in clickhouse server if not exist
func init() {

	connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?username=&debug=true&compress=1")
	if err != nil {
		log.Fatal(err)
	}
	{
		connect.Begin()
		stmt, _ := connect.Prepare(`
			CREATE TABLE IF NOT EXISTS Todo (
				Id        UInt8,
				Name      String,
				Completed      bool,
				Due   Date
			) engine=Memory
		`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}

}

func getTodos(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var todos []Todo

	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=&debug=true&compress=1")

	if err != nil {
		log.Fatal(err)
	}
	{

		rows, err := connect.Query(`
		SELECT
			Id,
			Name,
			Completed,
			Due
		FROM
			Todo`)

		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var todo Todo
			err := rows.Scan(&todo.Id, &todo.Name, &todo.Completed, &todo.Due)
			if err != nil {
				panic(err.Error())
			}
			todos = append(todos, todo)
		}
		json.NewEncoder(w).Encode(todos)

	}

}

func postTodo(w http.ResponseWriter, r *http.Request) {
	var todo Todo
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		panic(err)
	}
	if err := r.Body.Close(); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(body, &todo); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
	}

	t := RepoCreateTodo(todo)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(t); err != nil {
		panic(err)
	}
}

func RepoCreateTodo(t Todo) Todo {

	connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?username=&debug=true&compress=1")
	if err != nil {
		log.Fatal(err)
	}
	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare("INSERT INTO Todo (Id, Name, Completed, Due) VALUES (?, ?, ?, ?)")
		for i := 0; i < 1; i++ {
			if _, err := stmt.Exec([]driver.Value{
				t.Id,
				t.Name,
				t.Completed,
				t.Due,
			}); err != nil {
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	return t
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/todos", getTodos).Methods("GET")
	router.HandleFunc("/todo", postTodo).Methods("POST")

	log.Fatal(http.ListenAndServe(":8282", router))
}
