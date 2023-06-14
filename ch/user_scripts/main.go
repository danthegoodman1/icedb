package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	logout, err := os.Create("/tmp/out.log")
	if err != nil {
		log.Fatal("Error opening log out", err)
	}

	defer func() {
		if err := recover(); err != nil {
			log.Println("panic occurred:", err)
			log.Println(string(debug.Stack()))
		}
	}()

	log.SetOutput(logout)

	reader := bufio.NewReader(os.Stdin)
	str, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("error reading stdin:", err)
	}
	log.Println("got str:", str)
	args := strings.Split(str, "\t")

	conn, err := pgx.Connect(context.Background(), "postgresql://root@crdb:26257/defaultdb")
	if err != nil {
		log.Fatal("Unable to connect to database: ", err)
	}
	defer conn.Close(context.Background())

	syear, smonth, sday, eyear, emonth, eday := args[0], args[1], args[2], args[3], args[4], args[5]

	var files []string

	rows, err := conn.Query(context.Background(), `
	select partition, filename
	from known_files
	where active = true
	AND partition >= $1
	AND partition <= $2
	`, fmt.Sprintf("y=%04s/m=%02s/d=%02s", syear, smonth, sday), fmt.Sprintf("y=%04s/m=%02s/d=%02s", eyear, emonth, eday))
	if err != nil {
		log.Fatal("err querying", err)
	}

	log.Println("got part range", fmt.Sprintf("y=%04s/m=%02s/d=%02s", syear, smonth, sday), fmt.Sprintf("y=%04s/m=%02s/d=%02s", eyear, emonth, eday))

	for rows.Next() {
		log.Println("scanning row...")
		var partition, filename string
		err := rows.Scan(&partition, &filename)
		if err != nil {
			log.Fatal("error scanning rows:", err)
		}
		files = append(files, partition+"/"+filename)
	}

	out := fmt.Sprintf("http://minio:9000/testbucket/{%s}", strings.Join(files, ","))
	log.Println("writing out:", out)
	fmt.Print(out)
}
