package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status,omitempty"`
	Timestamp  string  `json:"timestamp,omitempty"`
	MethodType string  `json:"method_type,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

var producer sarama.SyncProducer

func initKafka(brokers []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	return err
}

func startConsumer(brokers []string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Printf("Failed to start consumer: %v", err)
		return
	}

	topics := []string{"movie-events", "user-events", "payment-events"}
	for _, topic := range topics {
		go consumeTopic(consumer, topic)
	}
}

func consumeTopic(consumer sarama.Consumer, topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to consume topic %s: %v", topic, err)
		return
	}
	defer partitionConsumer.Close()

	log.Printf("Consuming topic: %s", topic)
	for msg := range partitionConsumer.Messages() {
		log.Printf("[%s] partition=%d offset=%d value=%s", topic, msg.Partition, msg.Offset, string(msg.Value))
	}
}

func publish(topic string, event Event) (int32, int64, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(payload),
	}

	return producer.SendMessage(msg)
}

func handlerHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handlerMovie(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", payload.MovieID, payload.Action),
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publish("movie-events", event)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	})
}

func handlerUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload UserEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", payload.UserID, payload.Action),
		Type:      "user",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publish("user-events", event)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	})
}

func handlerPayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", payload.PaymentID, payload.Status),
		Type:      "payment",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publish("payment-events", event)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	})
}

func main() {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "localhost:9092"
	}

	if err := initKafka(brokers); err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer producer.Close()

	startConsumer(brokers)

	http.HandleFunc("/api/events/health", handlerHealth)
	http.HandleFunc("/api/events/movie", handlerMovie)
	http.HandleFunc("/api/events/user", handlerUser)
	http.HandleFunc("/api/events/payment", handlerPayment)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
