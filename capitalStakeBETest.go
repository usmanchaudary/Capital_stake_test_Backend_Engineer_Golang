package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

var datas = Load("covid_data.csv")

type QueryField struct {
	Query struct {
		Region string `json:"region"`
		Date   string `json:"date"`
	} `json:"query"`
}

func main() {
	var Address string
	var network string
	flag.StringVar(&Address, "e", ":4040", "service endpoint [ip Address or socket path]")
	flag.StringVar(&network, "n", "tcp", "network protocol [tcp,unix]")
	flag.Parse()

	// validate supported network protocols
	switch network {
	case "tcp", "tcp4", "tcp6", "unix":
	default:
		log.Fatalln("unsupported network protocol:", network)
	}

	// create a listener for provided network and host Addresses
	ln, err := net.Listen(network, Address)
	if err != nil {
		log.Fatal("failed to create listener:", err)
	}
	defer ln.Close()
	log.Println("**** Covid Data Service ***")
	log.Printf("Service started: (%s) %s\n", network, Address)

	// connection-loop - handle incoming requests
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			if err := conn.Close(); err != nil {
				log.Println("failed to close listener:", err)
			}
			continue
		}
		log.Println("Connected to", conn.RemoteAddr())

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("error closing connection:", err)
		}
	}()

	if _, err := conn.Write([]byte("Connected...\nUsage: JSON format input only\n")); err != nil {
		log.Println("error writing:", err)
		return
	}

	// loop to stay connected with client until client breaks connection
	for {
		// buffer for client command
		cmdLine := make([]byte, (1024 * 4))
		n, err := conn.Read(cmdLine)
		if n == 0 || err != nil {
			log.Println("connection read error:", err)
			return
		}

		var queryField QueryField

		errJson := json.Unmarshal([]byte(cmdLine[0:n]), &queryField)

		var param = ""
		if string(queryField.Query.Date) != "" {
			param = queryField.Query.Date
		} else if string(queryField.Query.Region) != "" {
			param = queryField.Query.Region
		}

		// execute command
		if errJson == nil && param != "" {
			result := Find(datas, param)
			if len(result) == 0 {
				if _, err := conn.Write([]byte("Nothing found\n")); err != nil {
					log.Println("failed to write:", err)
				}
				continue
			}
			// sending matched info to the client as json string
			jsonString, _ := json.MarshalIndent(result, "", " ")
			_, err := conn.Write([]byte(fmt.Sprintf(string(jsonString))))
			if err != nil {
				log.Println("failed to write response:", err)
				return
			}

		} else {

			if _, errIn := conn.Write([]byte("Invalid Input")); errIn != nil {
				log.Println("failed to write:", errIn)
				return
			}
		}

	}
}

func parseCommand(cmdLine string) (cmd, param string) {
	parts := strings.Split(cmdLine, " ")
	if len(parts) != 2 {
		return "", ""
	}
	cmd = strings.TrimSpace(parts[0])
	param = strings.TrimSpace(parts[1])
	return
}

//covid_data.csv loading
type CovidData struct {
	CumulativeTestPositive  string `json:"cumulativeTestPositive"`
	CumulativeTestPerformed string `json:"cumulativeTestPerformed"`
	Date                    string `json:"date"`
	Discharged              string `json:"discharged"`
	Expired                 string `json:"expired"`
	Region                  string `json:"region"`
	Admitted                string `json:"admitted"`
}

func Load(path string) []CovidData {
	table := make([]CovidData, 0)
	file, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	reader := csv.NewReader(file)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err.Error())
		}
		c := CovidData{
			CumulativeTestPositive:  row[0],
			CumulativeTestPerformed: row[1],
			Date:                    row[2],
			Discharged:              row[3],
			Expired:                 row[4],
			Region:                  row[5],
			Admitted:                row[6],
		}
		table = append(table, c)
	}
	return table
}

// Result Matching
func Find(table []CovidData, filter string) []CovidData {
	if filter == "" || filter == "*" {
		return table
	}
	result := make([]CovidData, 0)
	filter = strings.ToUpper(filter)
	for _, cur := range table {
		if strings.ToUpper(cur.Region) == filter ||
			strings.ToUpper(cur.Date) == filter {
			result = append(result, cur)
		}
	}
	return result
}
