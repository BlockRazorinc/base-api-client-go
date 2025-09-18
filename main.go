package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/BlockRazorinc/base-api-client-go/basepb"
	"github.com/andybalholm/brotli"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// Replace with the server address geographically closest to you for optimal performance.
	grpcAddr      = "tokyo.grpc.base.blockrazor.xyz:80"
	websocketAddr = "ws://tokyo.base.blockrazor.xyz:81/ws"
	//websocketAddr          = "ws://frankfurt.base.blockrazor.xyz:81/ws"
	//websocketAddr          = "ws://virginia.base.blockrazor.xyz:81/ws"
	authToken = "your token goes here" // Replace with your authentication token.
)

func main() {
	// main is the entry point of the program.
	// It directly calls one of the stream functions to demonstrate its usage.
	// The program will exit once the called function completes.
	// GetFlashBlockStream(authToken)
	GetBlockStream(authToken)
	// GetWebSocketFlashBlockStream(authToken)
	// sendTransactions(client, authToken, "0x + ....") // It's recommended to keep the gRPC client alive for sending transactions.
}

// GetBlockStream provides a simplified example of subscribing to and processing the regular block stream.
// Note: This function attempts to connect and subscribe only once. For production use, implement your own reconnection logic.
func GetBlockStream(authToken string) {
	log.Printf("[BlockStream] Attempting to connect to gRPC server at %s...", grpcAddr)

	// Establish a connection to the gRPC server with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[BlockStream] Failed to connect to gRPC server: %v", err)
		return
	}
	defer conn.Close()

	log.Println("[BlockStream] Successfully connected to gRPC server.")
	client := basepb.NewBaseApiClient(conn)

	// Create a new context with authentication metadata for the stream subscription.
	streamCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", authToken))
	stream, err := client.GetBlockStream(streamCtx, &basepb.GetBlockStreamRequest{})
	if err != nil {
		log.Printf("[BlockStream] Failed to subscribe to stream: %v", err)
		return
	}

	log.Println("[BlockStream] Subscription successful. Waiting for new blocks...")

	// Loop indefinitely to receive messages from the stream.
	for {
		block, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("[BlockStream] Stream closed by the server (EOF).")
			} else {
				log.Printf("[BlockStream] An error occurred while receiving data: %v", err)
			}
			break // Exit the loop on error or stream closure.
		}

		// Process the received block data.
		log.Printf("=> [BlockStream] Received new block: Number=%d, Hash=%s, TransactionCount=%d",
			block.GetBlockNumber(),
			block.GetBlockHash(),
			len(block.GetTransactions()),
		)
		// To decode transactions, you can call DecodeTransactions(block.Transactions).
	}
}

// GetFlashBlockStream provides a simplified example of subscribing to and processing the flash block stream.
// Note: This function attempts to connect and subscribe only once. For production use, implement your own reconnection logic.
func GetFlashBlockStream(authToken string) {
	log.Printf("[FlashStream] Attempting to connect to gRPC server at %s...", grpcAddr)

	// Establish a connection to the gRPC server with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[FlashStream] Failed to connect to gRPC server: %v", err)
		return
	}
	defer conn.Close()

	log.Println("[FlashStream] Successfully connected to gRPC server.")
	client := basepb.NewBaseApiClient(conn)

	// Create a new context with authentication metadata for the stream subscription.
	streamCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", authToken))
	stream, err := client.GetRawFlashBlockStream(streamCtx, &basepb.GetRawFlashBlocksStreamRequest{})
	if err != nil {
		log.Printf("[FlashStream] Failed to subscribe to stream: %v", err)
		return
	}

	log.Println("[FlashStream] Subscription successful. Waiting for new flash blocks...")

	// Loop indefinitely to receive messages from the stream.
	for {
		block, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("[FlashStream] Stream closed by the server (EOF).")
			} else {
				log.Printf("[FlashStream] An error occurred while receiving data: %v", err)
			}
			break // Exit the loop on error or stream closure.
		}

		// Process the received flash block data.
		jsonString, err := ParseFlashBlockByte(block.Message)
		if err != nil {
			log.Printf("[FlashStream] Failed to parse flash block data: %v", err)
			continue
		}

		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(jsonString), &jsonMap); err != nil {
			log.Printf("[FlashStream] Failed to unmarshal flash block JSON: %v", err)
			continue
		}
		printPretty(jsonMap)
	}
}

// sendTransactions is an example function for sending a transaction.
// It's designed to reuse an existing gRPC client connection to avoid connection latency.
func sendTransactions(client basepb.BaseApiClient, authToken string, rawTxString string) {
	log.Println("[SendTx] Sending transaction...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Add the authentication token to the outgoing request's metadata.
	md := metadata.Pairs("authorization", authToken)
	ctx = metadata.NewOutgoingContext(ctx, md)

	req := &basepb.SendTransactionRequest{
		RawTransaction: rawTxString,
	}

	res, err := client.SendTransaction(ctx, req)
	if err != nil {
		log.Printf("[SendTx] Failed to send transaction: %v", err)
	} else {
		log.Printf("[SendTx] Transaction sent successfully. Hash: %s", res.GetTxHash())
	}
}

// FlashBlockWebSocketResponse defines the outer JSON-RPC structure for WebSocket messages.
type FlashBlockWebSocketResponse struct {
	JsonRPC string `json:"jsonrpc"`
	Result  []byte `json:"result"`
}

// GetWebSocketFlashBlockStream provides a simplified example of using the WebSocket API to subscribe to the flash block stream.
// Note: This function attempts to connect only once and will exit on a read error. For production use, implement your own reconnection logic.
func GetWebSocketFlashBlockStream(authToken string) {
	// Set up the HTTP header with the authorization token.
	header := http.Header{}
	header.Set("Authorization", authToken)

	// Dial the WebSocket server.
	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.Dial(websocketAddr, header)
	if err != nil {
		if resp != nil {
			log.Fatalf("[WebSocket] Dial failed: %v (HTTP status: %s)", err, resp.Status)
		}
		log.Fatalf("[WebSocket] Dial failed: %v", err)
	}
	defer conn.Close()
	log.Printf("[WebSocket] Successfully connected to %s", websocketAddr)

	// Prepare the JSON-RPC subscription request.
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "subscribe_FlashBlock",
		"params":  []interface{}{},
		"id":      1,
	}
	reqB, _ := json.Marshal(req)
	if err := conn.WriteMessage(websocket.TextMessage, reqB); err != nil {
		log.Fatalf("[WebSocket] Failed to send subscription request: %v", err)
	}
	log.Printf("[WebSocket] Subscription request sent: %s", string(reqB))

	// Loop indefinitely to read messages from the server.
	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WebSocket] Error reading message: %v", err)
			return // Exit the function on any read error.
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue // Ignore messages that are not text or binary.
		}

		// Parse the outer JSON-RPC response wrapper.
		var outer = &FlashBlockWebSocketResponse{}
		if err := json.Unmarshal(msg, &outer); err != nil {
			log.Printf("[WebSocket] Failed to parse JSON from server: %v\nRaw data: %s", err, string(msg))
			continue
		}

		// Extract the "result" field, which contains the actual flash block data, and process it.
		resultRaw := outer.Result
		if jsonString, err := ParseFlashBlockByte(resultRaw); err == nil {
			printPretty(jsonString)
		} else {
			log.Printf("[WebSocket] Received message without a valid result field. Raw data: %s", string(msg))
		}
	}
}

// --- Utility Functions ---

// ParseFlashBlockByte decompresses the brotli-compressed binary data of a flash block.
func ParseFlashBlockByte(data []byte) (string, error) {
	br := brotli.NewReader(bytes.NewReader(data))
	var buf bytes.Buffer
	_, err := buf.ReadFrom(br)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

// DecodeTransactions decodes a slice of byte slices into go-ethereum Transaction objects.
func DecodeTransactions(txData [][]byte) ([]*types.Transaction, error) {
	transactions := make([]*types.Transaction, len(txData))
	for idx, txByte := range txData {
		tx := &types.Transaction{}
		err := tx.UnmarshalBinary(txByte)
		if err != nil {
			log.Printf("[DecodeTx] Error unmarshalling transaction binary: %v", err)
			return nil, err
		}
		log.Printf("[DecodeTx] Decoded transaction with hash: %v", tx.Hash())
		transactions[idx] = tx
	}
	return transactions, nil
}

// printPretty formats and prints a JSON string or any interface with indentation for readability.
func printPretty(v interface{}) {
	var out []byte
	var err error

	// If the input is already a string, attempt to unmarshal and re-marshal it for formatting.
	if s, ok := v.(string); ok {
		var js map[string]interface{}
		if json.Unmarshal([]byte(s), &js) == nil {
			out, err = json.MarshalIndent(js, "", "  ")
		} else {
			// If it's a string but not valid JSON, just print it directly.
			out = []byte(s)
		}
	} else {
		// If it's not a string, marshal it directly.
		out, err = json.MarshalIndent(v, "", "  ")
	}

	if err != nil {
		log.Printf("Failed to marshal for pretty printing, printing raw value: %#v\n", v)
		return
	}
	log.Println(string(out))
}
