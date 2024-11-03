package main

import (
	client "Code/client"
	replica "Code/replica"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func InitializeDNS (rep *replica.Replica) {
    rep.DNS = make(map[string]string)
	rep.DNS["Google"] = "127.0.0.0"
	rep.DNS["YouTube"] = "127.0.0.1"
	rep.DNS["Facebook"] = "127.0.0.2"
	rep.DNS["X"] = "127.0.0.3"
	rep.DNS["Wikipedia"] = "127.0.0.4"
	rep.DNS["Baidu"] = "127.0.0.5"
	rep.DNS["TikTok"] = "127.0.0.6"
	rep.DNS["Amazon"] = "127.0.0.7"
	rep.DNS["Yahoo"] = "127.0.0.8"
}

func Initialize(rep *replica.Replica, port int) {
    rep.ID = (port) % 5000
	rep.Address = "127.0.0." + strconv.Itoa(port)
	InitializeDNS(rep)
	rep.View = 0
	if rep.ID == 0 {
		rep.IsPrimary = true
	} else {
		rep.IsPrimary = false
	}
	rep.ViewChangeInProgress = false
	rep.CurLogID = 0;
	rep.ClientRequestID = make(map[int]int)
	rep.CommittedNumber = 0
	rep.Timeout = 5
	rep.HeartBeatInterval = 10*time.Second
	rep.ViewChangeCounts = make(map[int]int)
	rep.PrintedViews = make(map[int]bool)
}

func InitializeClient(client * client.Client, id int) {
	client.ID = id
	client.View = 0
	client.RequestID = 1
}

func main() {
	logFile, err := os.OpenFile("logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	var replicaSize int
	var clientSize int
	var wg sync.WaitGroup

	fmt.Print("Do you want to input from the terminal or the input file?\n")
	fmt.Print("Type 0 for terminal and 1 for input file.\n")
	var reply int
	_, err = fmt.Scan(&reply)
	if err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
	if reply == 0 {
		fmt.Print("Enter the number of Replicas: ")
		_, err = fmt.Scan(&replicaSize)
		if err != nil {
			log.Fatalf("Error reading input: %v", err)
		}

		fmt.Print("Enter the number of clients: ")
		_, err = fmt.Scan(&clientSize)
		if err != nil {
			log.Fatalf("Error reading input: %v", err)
		}

		replicasArray := make([]replica.Replica, replicaSize)
		clientsArray := make([]client.Client, clientSize)

		for i := 0; i < clientSize; i++ {
			fmt.Printf("Enter the number of requests of Client %d: ", i)
			var temp int
			_, err = fmt.Scan(&temp)
			if err != nil {
				log.Fatalf("Error reading input: %v", err)
			}
			for j := 0; j < temp; j++ {
				var typ int
				var key string
				var val string
				fmt.Scan(&typ)
				fmt.Scan(&key)
				fmt.Scan(&val)
				req := &client.Request{RequestType: typ, Key: key, Value: val}
				clientsArray[i].Requests = append(clientsArray[i].Requests, req)
			}
		}

		var canFail, actualFailures int
		if replicaSize%2 == 0 {
			canFail = (replicaSize-2)/2
		} else {
			canFail = (replicaSize)/2 - 1
		}
		var finalTimeout int
		marked := make(map[int]bool)
		fmt.Printf("%d replicas can fail. Enter the number of failures: ", canFail)
		fmt.Scan(&actualFailures)
		fmt.Printf("%d replicas can fail. Enter the Replica ID and the time of its failure.\n", canFail)
		for i := 0; i < actualFailures; i++ {
			var id, timeofFail int
			fmt.Scan(&id)
			fmt.Scan(&timeofFail)
			replicasArray[id].FailureTime = time.Duration(timeofFail)
			finalTimeout = max(finalTimeout, timeofFail)
			marked[id] = true
		}
		for i := 0; i < replicaSize; i++ {
			if !marked[i] {
				replicasArray[i].FailureTime = time.Duration(finalTimeout + 100)
			}
		}

		for i := 0; i < replicaSize; i++ {
			Initialize(&replicasArray[i], 5000 + i)
			wg.Add(1)
			go replicasArray[i].Run(&wg, 5000 + i, replicaSize)
		}

		for i := 0; i < clientSize; i++ {
			InitializeClient(&clientsArray[i], i)
			go clientsArray[i].Run()
		}

		wg.Wait()

		for i := 0; i < replicaSize; i++ {
			if replicasArray[i].IsPrimary {
				replicasArray[i].PrintDetails()
				break
			}
		}
	} else {
		file, err := os.Open("input.txt")
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		defer file.Close() 
		
		fmt.Fscanf(file, "%d\n", &replicaSize)
		fmt.Fscanf(file, "%d\n", &clientSize)
		fmt.Printf("%d %d\n", replicaSize, clientSize)
		replicasArray := make([]replica.Replica, replicaSize)
		clientsArray := make([]client.Client, clientSize)

		for i := 0; i < clientSize; i++ {
			var temp int
			fmt.Fscanf(file, "%d\n", &temp)
			fmt.Printf("%d\n", temp)
			for j := 0; j < temp; j++ {
				var typ int
				var key string
				var val string
				fmt.Fscanf(file, "%d %s %s\n", &typ, &key, &val)
				fmt.Printf("%d %s %s\n", typ, key, val)
				req := &client.Request{RequestType: typ, Key: key, Value: val}
				clientsArray[i].Requests = append(clientsArray[i].Requests, req)
			}
		}

		for i := 0; i < replicaSize; i++ {
			Initialize(&replicasArray[i], 5000 + i)
			wg.Add(1)
			go replicasArray[i].Run(&wg, 5000 + i, replicaSize)
		}

		for i := 0; i < clientSize; i++ {
			InitializeClient(&clientsArray[i], i)
			go clientsArray[i].Run()
		}

		wg.Wait()

		for i := 0; i < replicaSize; i++ {
			if replicasArray[i].IsPrimary {
				replicasArray[i].PrintDetails()
				break
			}
		}
	}
	fmt.Println("Successfull!!")
}