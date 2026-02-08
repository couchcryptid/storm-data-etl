//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestKafkaPipeline_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	stack, err := compose.NewDockerComposeWith(compose.WithStackFiles(filepath.Join("..", "..", "compose.yml")))
	require.NoError(t, err)

	err = stack.
		WaitForService("etl", wait.ForListeningPort("8080/tcp")).
		Up(ctx, compose.Wait(true))
	require.NoError(t, err)
	defer func() {
		_ = stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true))
	}()

	brokers := []string{"localhost:9092"}
	require.NoError(t, waitForKafka(ctx, brokers[0]))

	sourceTopic := "raw-weather-reports"
	sinkTopic := "transformed-weather-data"
	require.NoError(t, ensureTopics(brokers[0], sourceTopic, sinkTopic))

	reader := newSinkReader(brokers, sinkTopic)
	defer func() {
		_ = reader.Close()
	}()

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    sourceTopic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer func() {
		_ = writer.Close()
	}()

	begin := time.Date(2024, time.April, 26, 12, 23, 0, 0, time.UTC)
	input := domain.StormEvent{
		ID:        "it-1",
		EventType: "tornado",
		Geo:       domain.Geo{Lat: 34.96, Lon: -95.77},
		Magnitude: 2,
		Unit:      "f_scale",
		BeginTime: begin,
		EndTime:   begin,
		Source:    "integration",
		Location: domain.Location{
			Raw:    "2 N Mcalester",
			State:  "OK",
			County: "Pittsburg",
		},
		Comments: "Test tornado report. (TSA)",
	}
	payload, err := json.Marshal(input)
	require.NoError(t, err)

	require.NoError(t, writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(input.ID),
		Value: payload,
	}))

	received, err := readTransformed(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, "tornado", received.EventType)
	require.Equal(t, "OK", received.Location.State)
	require.Equal(t, "Pittsburg", received.Location.County)
	require.Equal(t, "Mcalester", received.Location.Name)
	require.Equal(t, "TSA", received.SourceOffice)
	require.Equal(t, "2024-04-26T12:00:00Z", received.TimeBucket)
}

func waitForKafka(ctx context.Context, address string) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		dialer := &net.Dialer{Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return errors.New("kafka broker not reachable before timeout")
}

func newSinkReader(brokers []string, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     fmt.Sprintf("integration-test-%d", time.Now().UnixNano()),
		StartOffset: kafkago.LastOffset,
	})
}

func ensureTopics(broker string, topics ...string) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer func() {
		_ = controllerConn.Close()
	}()

	configs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafkago.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	if err := controllerConn.CreateTopics(configs...); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create topics: %w", err)
	}

	return nil
}

func readTransformed(ctx context.Context, reader *kafkago.Reader) (domain.StormEvent, error) {
	readCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		return domain.StormEvent{}, fmt.Errorf("read transformed: %w", err)
	}

	var event domain.StormEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return domain.StormEvent{}, fmt.Errorf("unmarshal transformed: %w", err)
	}

	return event, nil
}
