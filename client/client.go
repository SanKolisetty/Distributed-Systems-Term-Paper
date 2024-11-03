package client

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	pb "Code/proto/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Request struct {
	RequestType int    // 0 for Read, 1 for Write
	Key         string // Key
	Value       string // Value
	Modified    bool   // Modified is true when the operation is successful
}

type Client struct {
	ID        int
	View      int
	RequestID int
	Requests  []*Request
}

var (
	addr = "localhost:5000"
)

func (client *Client) Run() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewReplicaServerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	// REQUESTS FROM MAIN.GO

	// Define the number of retries allowed before incrementing the view
	const maxRetries = 3

	for i := 0; i < len(client.Requests); i++ {
		randomInt := rand.Intn(2) + 1
		time.Sleep(time.Duration(randomInt) * time.Second)
		var err error
		// Retry loop for each request, attempting up to maxRetries times
		for attempt := 1; attempt <= maxRetries; attempt++ {
			if client.Requests[i].RequestType == 0 {
				// Read request
				log.Printf("Client %d: Sent request, RequestID: %d, asking to read the IP address of %s (Attempt %d)\n",
					client.ID, client.RequestID, client.Requests[i].Key, attempt)
				// Perform the read operation
				var r *pb.ReadReply
				r, err = c.Read(ctx, &pb.ReadRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: client.Requests[i].Key})
				if err == nil {
					// Success: Log the reply and break out of retry loop
					log.Printf("Client %d: Received Reply: %s\n", client.ID, r.GetValue())
					break
				}
			} else {
				// Write request
				log.Printf("Client %d: Sent request, RequestID: %d, asking to modify the IP address of %s to %s (Attempt %d)\n",
					client.ID, client.RequestID, client.Requests[i].Key, client.Requests[i].Value, attempt)
				// Perform the write operation
				var r *pb.WriteReply
				r, err = c.Write(ctx, &pb.WriteRequest{Requestid: int64(client.RequestID), Clientid: int64(client.ID), Websitename: client.Requests[i].Key, Address: client.Requests[i].Value})
				if err == nil {
					// Success: Log the reply and break out of retry loop
					log.Printf("Client %d: Received Reply: %v\n", client.ID, r.GetModified())
					break
				}
			}
			// Log the error and wait before the next attempt
			log.Printf("Client %d: Request failed (Attempt %d/%d): %v", client.ID, attempt, maxRetries, err)
			if attempt == maxRetries {
				break
			}
			time.Sleep(5 * time.Second) // Wait before retrying
		}

		// After max retries, if err still exists, increment the view and break out of the main loop
		if err != nil {
			log.Printf("Client %d: Failed after %d attempts, increasing view to %d\n", client.ID, maxRetries, client.View+1)
			client.View++
			conn, err = grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+client.View), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Client %d: Couldn't connect to the next Replica i.e Replica %d", client.ID, client.View)
				log.Printf("Client %d: Exiting", client.ID)
				os.Exit(0)
			}
			c = pb.NewReplicaServerClient(conn)
		} else {
			// Increment the client request ID for the next request
			client.RequestID++
		}
	}

	log.Printf("Client %d completed", client.ID)
}
