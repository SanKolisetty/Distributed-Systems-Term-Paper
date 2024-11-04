package replica

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	pb "Code/proto/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Struct for Replica
type Replica struct {
	ID                   int                       // ID of the replica
	Address              string                    // Address of the replica
	DNS                  map[string]string         // DNS stored at the replica
	Log                  []*LogEntry               // Log Entries
	View                 int                       // Current view
	IsPrimary            bool                      // Whether it's primary or not
	ViewChangeInProgress bool                      // Whether the view change is in progress or not
	CurLogID             int                       // Current Log ID: The index at which the new one can be added (initialized with 0)
	ClientRequestID      map[int]int               // Maintains the recent most request ID of a client
	CommittedNumber      int                       // Committed Number
	Timeout              int                       // Timeout value
	activityChan         chan struct{}             // Channel to signal activity (heartbeat reception)
	HeartBeatInterval    time.Duration             // HeartBeatInterval
	FailureTime          time.Duration             // The server stays up for this much time and then stops/fails
	ViewChangeCounts     map[int]int               // Map to track counts of requests for each view number
	PrintedViews         map[int]bool              // Map to track if a view has been printed
	DoViewChangeCount    []*pb.DoViewChangeRequest // DoViewChange Requests received
}

// Struct for containing all the details of a Log Entry
type LogEntry struct {
	LogID     int    // Log ID: Index of the entry
	RequestID int    // Request ID at the client
	ClientID  int    // Client ID of the client that sent the request
	Operation int    // Type of operation
	Key       string // Website Name
	Value     string // Website Address
	Committed bool   // Whether it's committed or not
}

// Struct for a server
type server struct {
	pb.UnimplementedReplicaServerServer
	rep         *Replica
	replicaSize int             // Number of replicas in the system
	f           int             // Failure threshold
	mu          sync.Mutex      // Mutex lock
	ctx         context.Context // Context
	wg          *sync.WaitGroup // WaitGroup
}

// Incrementing Log ID
func (s *server) IncrementingLogID(requestid int, clientid int, websitename string, value string) (*LogEntry, error) {
	// Locking
	s.mu.Lock()
	defer s.mu.Unlock()
	// Creating a log entry
	logentry := &LogEntry{
		LogID:     s.rep.CurLogID,
		RequestID: requestid,
		ClientID:  clientid,
		Operation: 1,
		Key:       websitename,
		Value:     value,
	}
	// Appending to the Log
	s.rep.Log = append(s.rep.Log, logentry)
	// Updating the Client's latest Request ID
	s.rep.ClientRequestID[clientid] = requestid
	// Incrementing the current Log ID
	s.rep.CurLogID++
	return logentry, nil
}

// Incrementing Commit Number
func (s *server) IncrementingCommitNumber(logentry *LogEntry) {
	// Locking
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.CommittedNumber = logentry.LogID     // Committing till the current entry
	s.rep.DNS[logentry.Key] = logentry.Value   // Updating the DNS
	s.rep.Log[logentry.LogID].Committed = true // Marking it as 'Committed'
}

// Read RPC
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadReply, error) {
	if s.rep.IsPrimary {
		log.Printf("Replica %d: Received Read Request from Client: %d, with request ID: %d and Website Name: %s\n", s.rep.ID, req.GetClientid(), req.GetRequestid(), req.GetWebsitename())
		// Checks whether the Website exists in the DNS and sends a reply accordingly
		ip, exists := s.rep.DNS[req.GetWebsitename()]
		if exists {
			return &pb.ReadReply{Value: ip}, nil
		}
		return &pb.ReadReply{Value: "NA"}, nil
	}
	return nil, status.Errorf(codes.FailedPrecondition, "Replica is not the primary")
}

// Write RPC
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteReply, error) {
	if s.rep.IsPrimary {
		log.Printf("Replica %d: Received Write Request from Client: %d, with request: %d, Website Name: %s and New Address: %s\n", s.rep.ID, req.GetClientid(), req.GetRequestid(), req.GetWebsitename(), req.GetAddress())
		val, exist := s.rep.ClientRequestID[int(req.Clientid)]
		if exist { // Checks the Request ID with the one stored at the replica
			if val > int(req.Requestid) {
				return &pb.WriteReply{Modified: false}, fmt.Errorf("this request has already been completed") // Request is less than the recently finished one
			}
			if val == int(req.Requestid) {
				return &pb.WriteReply{Modified: true}, nil // Request is the recently finished one
			}
		}
		// New request
		addr, exists := s.rep.DNS[req.GetWebsitename()]
		if addr == req.GetAddress() {
			// The modification is already in place
			return &pb.WriteReply{Modified: false}, nil
		}
		// When the website exists
		if exists {
			// Create a logentry
			logentry, _ := s.IncrementingLogID(int(req.GetRequestid()), int(req.GetClientid()), req.GetWebsitename(), req.GetAddress())
			// Sending Prepare messages to all other replicas and receiving at least f+1 responses
			s.broadcastVerify(int64(s.rep.CurLogID), req.GetRequestid(), req.GetClientid(), req.GetWebsitename(), req.GetAddress())
			// for s.rep.CommittedNumber + 1 != logentry.LogID {
			// }
			s.IncrementingCommitNumber(logentry)
			log.Printf("Replica %d: Received %d number of OKs from the replicas for Client %d's request with Request ID: %d!", s.rep.ID, s.f, logentry.ClientID, logentry.LogID)
			return &pb.WriteReply{Modified: true}, nil
		}
		// When the website doesn't exist in the DNS
		return &pb.WriteReply{Modified: false}, fmt.Errorf("website doesn't exist in the DNS. please check and try again")
	}
	return nil, status.Errorf(codes.FailedPrecondition, "Replica is not the primary")
}

// Sending Prepare Requests to all replicas
func (s *server) broadcastVerify(logid, requestID, clientID int64, websiteName, address string) {
	var wg sync.WaitGroup
	doneCh := make(chan bool, s.replicaSize)
	threshold := s.f - 1

	for i := 0; i < s.replicaSize; i++ {
		if i != s.rep.View {
			wg.Add(1)
			go func(replicaID int) {
				defer wg.Done()
				// Connect to the replica
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+replicaID), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("Replica %d: Error connecting to Replica %d", s.rep.ID, replicaID)
					return
				}
				defer conn.Close()
				log.Printf("Replica %d: Sending Prepare for Client %d's request with Request ID: %d to Replica %d with LogID: %d", s.rep.ID, clientID, requestID, replicaID, logid)
				client := pb.NewReplicaServerClient(conn)
				// Send a Prepare Request
				resp, err := client.Prepare(context.Background(), &pb.PrepareRequest{
					Logid:        logid,
					Requestid:    requestID,
					Clientid:     clientID,
					Viewnumber:   int64(s.rep.View),
					Websitename:  websiteName,
					Address:      address,
					Commitnumber: int64(s.rep.CommittedNumber),
				})
				if err == nil && resp.GetReceived() {
					doneCh <- true
				} else {
					log.Printf("Replica %d: Failed to send Prepare to Replica %d", s.rep.ID, replicaID)
				}
			}(i)
		}
	}

	for i := 0; i < threshold; i++ {
		select {
		case <-doneCh:
			log.Printf("Replica %d: Received one OK for Client %d's Prepare request for request ID %d!", s.rep.ID, clientID, requestID)
		case <-time.After(500 * time.Second):
			log.Printf("Replica %d: Timeout waiting for response for Client %d's Prepare request for request ID %d!", s.rep.ID, clientID, requestID)
			return
		}
	}

	wg.Wait()
}

// Prepare RPC
func (s *server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareOKReply, error) {
	log.Printf("Replica %d: Received Prepare Request from Replica %d for Client %d's request with Request ID: %d", s.rep.ID, req.GetViewnumber(), req.GetClientid(), req.GetRequestid())
	// Create a Log Entry
	logentry := &LogEntry{LogID: int(req.GetLogid()), RequestID: int(req.GetRequestid()), ClientID: int(req.GetClientid()), Operation: 1, Key: req.GetWebsitename(), Value: req.GetAddress(), Committed: false}
	if s.rep.View == int(req.GetViewnumber()) {
		log.Printf("Replica %d: %d is the logsize and received a request with logID: %d", s.rep.ID, len(s.rep.Log), logentry.LogID)
		for len(s.rep.Log)+1 != logentry.LogID {
		}
		// s.rep.CurLogID = logentry.LogID + 1
		s.rep.CurLogID = logentry.LogID 
		s.rep.Log = append(s.rep.Log, logentry)
		s.rep.ClientRequestID[logentry.ClientID] = logentry.RequestID
		for i := min(req.Commitnumber-1, int64(len(s.rep.Log))); i >= 0; i-- {
			if !s.rep.Log[i].Committed {
				s.rep.DNS[s.rep.Log[i].Key] = s.rep.Log[i].Value
				s.rep.Log[i].Committed = true
			} else {
				break
			}
		}
		log.Printf("Replica %d: Sent PrepareOK Reply to Replica %d for Client %d's request with Request ID: %d", s.rep.ID, req.GetViewnumber(), req.GetClientid(), req.GetRequestid())
		return &pb.PrepareOKReply{Received: true}, nil
	}
	return &pb.PrepareOKReply{Received: false}, nil
}

// Commit RPCs: Commits the entries
func (s *server) Commit(ctx context.Context, req *pb.CommitRequest) (*emptypb.Empty, error) {
	for len(s.rep.Log) < int(req.GetCommitnumber()) {
	}
	for i := s.rep.CommittedNumber; i <= int(req.GetCommitnumber()); i++ {
		s.rep.Log[i].Committed = true
	}
	s.rep.CommittedNumber = int(req.GetCommitnumber())
	return &emptypb.Empty{}, nil
}

// Function to print the current details of a Replica
func (rep *Replica) PrintDetails() {
	log.Printf("Replica %d: Printing the final details of the Primary Replica", rep.ID)
	log.Printf("ID: %d\n", rep.ID)
	log.Printf("Address: %s\n", rep.Address)
	log.Println("DNS:")
	for key, value := range rep.DNS {
		log.Printf("Key: %s, Value: %s\n", key, value)
	}
	log.Printf("View: %d\n", rep.View)
	log.Printf("IsPrimary: %v\n", rep.IsPrimary)
	for i := 0; i < len(rep.Log); i++ {
		log.Printf("Log ID: %d, Key: %s, Value: %v\n", rep.Log[i].LogID, rep.Log[i].Key, rep.Log[i].Value)
	}
}

// Implement the ReceiveHeartBeat function to handle heartbeats
func (s *server) ReceiveHeartBeat(ctx context.Context, req *pb.HeartBeatRequest) (*emptypb.Empty, error) {
	if !s.rep.IsPrimary { // Only process heartbeats if not primary
		// Check if the heartbeat is from the current primary
		if int(req.ServerId) == s.rep.View {
			// Reset the inactivity timer on receiving a heartbeat from the primary
			log.Printf("Replica %d: The request's server ID is %d and the current Primary ID is %d", s.rep.ID, req.ServerId, s.rep.View)
			select {
			case s.rep.activityChan <- struct{}{}:
				log.Printf("Replica %d: Received a heartbeat from Replica %d which is the primary", s.rep.ID, req.ServerId)
				// Successfully reset the timer
			default:
				// Channel already has a signal, no need to add another
			}
		}
	}
	return &emptypb.Empty{}, nil // Return empty response
}

// checkInactivity monitors the inactivity timer and logs when no heartbeat is received.
func (s *server) checkInactivity(wg *sync.WaitGroup) {
	defer wg.Done()
	inactivityTimeout := s.rep.HeartBeatInterval // Use HeartBeatInterval as the inactivity timeout

	for {
		select {
		case <-time.After(inactivityTimeout):
			// Inactivity timeout reached; check if this is a non-primary replica
			if !s.rep.IsPrimary {
				if s.rep.ViewChangeInProgress {
					continue
				}
				log.Printf("Replica %d: No activity detected - no heartbeat received from primary\n", s.rep.ID)
				s.sendStartViewChange()
			}

		case <-s.rep.activityChan:
			// Reset the inactivity timer upon receiving a heartbeat
			// If a heartbeat is received, we don't need to log inactivity
			continue
		case <-s.ctx.Done():
			return
		}
	}
}

// sendHeartbeats regularly sends heartbeat messages to the non-primary replicas.
func (s *server) sendHeartbeats(wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(s.rep.HeartBeatInterval / 2)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.rep.IsPrimary {
				for i := 1; i < s.replicaSize; i++ {
					if i != s.rep.ID {
						conn, err := grpc.Dial("localhost:"+strconv.Itoa(5000+i), grpc.WithInsecure())
						if err != nil {
							log.Printf("Could not connect to replica %d: %v", i, err)
							continue
						}
						client := pb.NewReplicaServerClient(conn)
						_, err = client.ReceiveHeartBeat(context.Background(), &pb.HeartBeatRequest{ServerId: int64(s.rep.ID)})
						if err != nil {
							log.Printf("Error sending heartbeat to replica %d: %v", i, err)
						} else {
							log.Printf("Replica %d: Sent heartbeat to replica %d", s.rep.ID, i)
						}
						conn.Close()
					}
				}
			}
		}
		// time.Sleep(s.rep.HeartBeatInterval / 2) // Sleep for the heartbeat interval
	}
}

// Sends StartViewChange to all the replicas
func (s *server) sendStartViewChange() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rep.ViewChangeInProgress = true
	s.rep.View++
	for i := 0; i < s.replicaSize; i++ {
		if i != s.rep.View-1 {
			go func(replicaID int) {
				// Connect to the replica
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+replicaID), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("Replica %d: Error connecting to Replica %d", s.rep.ID, replicaID)
					return
				}
				defer conn.Close()

				client := pb.NewReplicaServerClient(conn)
				_, err = client.StartViewChange(context.Background(), &pb.StartViewChangeRequest{Replicaid: int64(s.rep.ID), Viewnumber: int64(s.rep.View)})
				if err != nil {
					log.Printf("Replica %d: Failed to send StartViewChange to Replica %d for view %d", s.rep.ID, replicaID, s.rep.View)
				} else {
					log.Printf("Replica %d: Sent StartViewChange to Replica %d for view %d", s.rep.ID, replicaID, s.rep.View)
				}
			}(i)
		}
	}
}

// StartViewChange handles incoming StartViewChange requests and counts requests per view number
func (s *server) StartViewChange(ctx context.Context, req *pb.StartViewChangeRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	view := int(req.Viewnumber)
	if view >= s.rep.View {
		// s.rep.View = view
		log.Printf("Replica %d: Received StartViewChange requests for view %d from Replica %d", s.rep.ID, req.Viewnumber, req.Replicaid)
		// If this view has already been printed, ignore further requests for it
		if s.rep.PrintedViews[view] {
			return &emptypb.Empty{}, nil
		}
		// Increment the count for this view number
		s.rep.ViewChangeCounts[view]++
		// Check if the count has reached the threshold `f`
		if s.rep.ViewChangeCounts[view] >= s.f {
			s.rep.ViewChangeInProgress = true
			// Print message and mark the view as processed
			log.Printf("Replica %d: Received %d StartViewChange requests for view %d", s.rep.ID, s.f, view)
			s.rep.PrintedViews[view] = true // Mark this view as processed
			if s.rep.View != s.rep.ID {
				log.Printf("Replica %d: Sending DoViewChange to the new primary %d", s.rep.ID, req.Viewnumber)
				go s.sendDoViewChange()
			}
		}
	}
	return &emptypb.Empty{}, nil
}

// Function to convert an array of protobuf LogEntry messages to an array of pointers to custom LogEntry structs
func convertProtoLogEntries(protoEntries []*pb.LogEntry) []*LogEntry {
	// Initialize a slice to hold pointers to the converted entries
	entries := make([]*LogEntry, len(protoEntries))
	// Iterate over each protobuf LogEntry and convert it
	for i, protoEntry := range protoEntries {
		// Create a new LogEntry and take its address
		entries[i] = &LogEntry{
			LogID:     int(protoEntry.LogID),
			RequestID: int(protoEntry.RequestID),
			ClientID:  int(protoEntry.ClientID),
			Operation: int(protoEntry.Operation),
			Key:       protoEntry.Key,
			Value:     protoEntry.Value,
			Committed: protoEntry.Committed,
		}
	}
	return entries
}

// Function to convert an array of custom LogEntry structs to an array of protobuf LogEntry messages
func convertToProtoLogEntries(entries []*LogEntry) []*pb.LogEntry {
	// Initialize a slice to hold the converted protobuf entries
	protoEntries := make([]*pb.LogEntry, len(entries))
	// Iterate over each LogEntry and convert it
	for i, entry := range entries {
		// Create a new protobuf LogEntry
		protoEntries[i] = &pb.LogEntry{
			LogID:     int64(entry.LogID),
			RequestID: int64(entry.RequestID),
			ClientID:  int64(entry.ClientID),
			Operation: int64(entry.Operation),
			Key:       entry.Key,
			Value:     entry.Value,
			Committed: entry.Committed,
		}
	}
	return protoEntries
}

// Helper function to convert map[int]int to map[int64]int64
func convertMapIntToInt64(input map[int]int) map[int64]int64 {
	result := make(map[int64]int64, len(input))
	for key, value := range input {
		result[int64(key)] = int64(value)
	}
	return result
}

// Helper function to convert map[int64]int64 to map[int]int
func convertMapInt64ToInt(input map[int64]int64) map[int]int {
	result := make(map[int]int, len(input))
	for key, value := range input {
		result[int(key)] = int(value)
	}
	return result
}

// Sends DoViewChange to the new found primary
func (s *server) sendDoViewChange() {
	var logents []*pb.LogEntry
	for i := 0; i < len(s.rep.Log); i++ {
		logent := &pb.LogEntry{
			LogID:     int64(s.rep.Log[i].LogID),
			RequestID: int64(s.rep.Log[i].RequestID),
			ClientID:  int64(s.rep.Log[i].ClientID),
			Operation: int64(s.rep.Log[i].Operation),
			Key:       s.rep.Log[i].Key,
			Value:     s.rep.Log[i].Value,
			Committed: s.rep.Log[i].Committed,
		}
		logents = append(logents, logent)
	}

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+s.rep.View), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Replica %d: Error connecting to Replica %d", s.rep.ID, s.rep.View)
		time.Sleep(time.Duration(rand.Intn(3)+1))
		go s.sendStartViewChange()
		return
	}
	defer conn.Close()
	client := pb.NewReplicaServerClient(conn)
	t := time.Duration(rand.Intn(2) + 1)
	time.Sleep(t * time.Second)
	log.Printf("Replica %d: Calling DoViewChange now to Replica %d", s.rep.ID, s.rep.View)
	_, err = client.DoViewChange(context.Background(), &pb.DoViewChangeRequest{
		Replicaid:       int64(s.rep.ID),
		Viewnumber:      int64(s.rep.View),
		Prevviewnumber:  int64(s.rep.View - 1),
		Opnumber:        int64(s.rep.CurLogID),
		Commitnumber:    int64(s.rep.CommittedNumber),
		Items:           logents,
		Clientrequestid: convertMapIntToInt64(s.rep.ClientRequestID),
		DNS: s.rep.DNS,
	})
	if err != nil {
		log.Printf("Replica %d: Failed to send DoViewChange to Replica %d for view %d: %v", s.rep.ID, s.rep.View, s.rep.View, err)
		time.Sleep(time.Duration(rand.Intn(3)+1))
		go s.sendStartViewChange()
		return
	}
}

// DoViewChange RPC
func (s *server) DoViewChange(ctx context.Context, req *pb.DoViewChangeRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("Replica %d: Received view change request from %d with Viewnumber: %d", s.rep.ID, req.Replicaid, req.Viewnumber)
	if s.rep.ID == int(req.Viewnumber) {
		log.Printf("Replica %d: Processing view change request from %d with Viewnumber: %d", s.rep.ID, req.Replicaid, req.Viewnumber)
		s.rep.DoViewChangeCount = append(s.rep.DoViewChangeCount, req)
		if len(s.rep.DoViewChangeCount) >= s.f {
			s.rep.View = int(s.rep.DoViewChangeCount[0].Viewnumber)
			selected := &pb.DoViewChangeRequest{Prevviewnumber: -1}
			for i := 0; i < len(s.rep.DoViewChangeCount); i++ {
				if s.rep.DoViewChangeCount[i].Prevviewnumber > selected.Prevviewnumber {
					selected = s.rep.DoViewChangeCount[i]
				} else if s.rep.DoViewChangeCount[i].Prevviewnumber == selected.Prevviewnumber {
					if s.rep.DoViewChangeCount[i].Opnumber > selected.Opnumber {
						selected = s.rep.DoViewChangeCount[i]
					}
				}
			}
			if !s.rep.IsPrimary {
				s.rep.CurLogID = int(selected.Opnumber)
				s.rep.CommittedNumber = int(selected.Commitnumber)
				s.rep.Log = convertProtoLogEntries(selected.Items)
				s.rep.ViewChangeInProgress = false
				s.rep.IsPrimary = true
				s.rep.ClientRequestID = convertMapInt64ToInt(selected.Clientrequestid)
				s.rep.DNS = selected.DNS
				log.Printf("Replica %d: Have become the primary now because of the request from %d", s.rep.ID, req.Replicaid)
				log.Printf("Replica %d: Sending StartView to all the replicas", s.rep.ID)
				s.wg.Add(1)
				go s.sendHeartbeats(s.wg)
				go s.sendStartView()
			} else {
				log.Printf("Replica %d: Have already become the primary, so won't be accepting the request from %d", s.rep.ID, req.Replicaid)
			}
		}
	} else {
		log.Printf("Replica %d: Have already become the primary, so won't be accepting the request from %d", s.rep.ID, req.Replicaid)
	}
	return &emptypb.Empty{}, nil
}

// Sends StartView to all replicas; sent by the new primary 
// to show that it has become the new primary 
func (s *server) sendStartView() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < s.replicaSize; i++ {
		if i != s.rep.View {
			go func(replicaID int) {
				// Connect to the replica
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 5000+replicaID), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("Replica %d: Error connecting to Replica %d", s.rep.ID, replicaID)
					return
				}
				defer conn.Close()

				client := pb.NewReplicaServerClient(conn)
				_, err = client.StartView(context.Background(), &pb.StartViewRequest{
					Viewnumber:   int64(s.rep.View),
					Opnumber:     int64(s.rep.CurLogID),
					Commitnumber: int64(s.rep.CommittedNumber),
					Log:          convertToProtoLogEntries(s.rep.Log),
					Clientrequestid: convertMapIntToInt64(s.rep.ClientRequestID),
					DNS: s.rep.DNS,
				})
				if err != nil {
					log.Printf("Replica %d: Failed to send StartView to Replica %d", s.rep.ID, replicaID)
				} else {
					log.Printf("Replica %d: Successfully sent StartView to Replica %d", s.rep.ID, replicaID)
				}
			}(i)
		}
	}
}

// StartView RPC: Sent by the new primary to all
// the other replicas
func (s *server) StartView(ctx context.Context, req *pb.StartViewRequest) (*emptypb.Empty, error) {
	s.rep.ViewChangeInProgress = false
	s.rep.View = int(req.Viewnumber)
	s.rep.CurLogID = int(req.Opnumber)
	s.rep.CommittedNumber = int(req.Commitnumber)
	s.rep.Log = convertProtoLogEntries(req.Log)
	s.rep.ClientRequestID = convertMapInt64ToInt(req.Clientrequestid)
	s.rep.DNS = req.DNS
	log.Printf("Replica %d: Received the new Primary Replica %d's StartView!", s.rep.ID, req.Viewnumber)
	return &emptypb.Empty{}, nil
}

// Run method for Replica
func (rep *Replica) Run(wg *sync.WaitGroup, port int, replicaSize int) {
	defer wg.Done()
	rand.Seed(time.Now().UnixNano())
	// Listener setup
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	rep.activityChan = make(chan struct{}, 1)

	// Set a timeout for 40 seconds
	timeout := rep.FailureTime * time.Second
	// if rep.IsPrimary {
	// 	timeout = 10 * time.Second
	// }
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var serverWG sync.WaitGroup

	// gRPC server initialization
	s := &server{
		rep:         rep,
		replicaSize: replicaSize,
		f:           replicaSize/2 + 1,
		ctx:         ctx,
		wg:          &serverWG,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterReplicaServerServer(grpcServer, s)

	// Start heartbeat and inactivity monitoring goroutines if primary
	if rep.IsPrimary {
		s.wg.Add(1)
		go s.sendHeartbeats(s.wg)
	}

	s.wg.Add(1)
	go s.checkInactivity(s.wg)

	// Start the gRPC server in a goroutine
	go func() {
		log.Printf("gRPC server listening on port: %d", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for timeout or external stop signal
	<-ctx.Done()

	// Gracefully stop the gRPC server and wait for goroutines to finish
	grpcServer.GracefulStop()
	log.Printf("gRPC server on port %d stopped gracefully", port)
	s.wg.Wait()
	log.Println("All background processes completed. Server fully shut down.")
}
