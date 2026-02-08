package domain

import (
	"context"
	"time"
)

// RawEvent represents an unprocessed message from the source topic.
type RawEvent struct {
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Commit    func(ctx context.Context) error
}

type Location struct {
	Raw       string  `json:"raw,omitempty"`
	Name      string  `json:"name,omitempty"`
	Distance  float64 `json:"distance,omitempty"`
	Direction string  `json:"direction,omitempty"`
	State     string  `json:"state,omitempty"`
	County    string  `json:"county,omitempty"`
}

type Geo struct {
	Lat float64 `json:"lat,omitempty"`
	Lon float64 `json:"lon,omitempty"`
}

// StormEvent is the domain-rich representation after parsing.
type StormEvent struct {
	ID           string    `json:"id"`
	EventType    string    `json:"type"`
	Geo          Geo       `json:"geo,omitempty"`
	Magnitude    float64   `json:"magnitude"`
	Unit         string    `json:"unit"`
	BeginTime    time.Time `json:"begin_time"`
	EndTime      time.Time `json:"end_time"`
	Source       string    `json:"source"`
	Location     Location  `json:"location,omitempty"`
	Comments     string    `json:"comments,omitempty"`
	Severity     string    `json:"severity,omitempty"`
	SourceOffice string    `json:"source_office,omitempty"`
	TimeBucket   string    `json:"time_bucket,omitempty"`
	RawPayload   []byte    `json:"-"`
	ProcessedAt  time.Time `json:"processed_at"`
}

// OutputEvent is the serialized form destined for the sink topic.
type OutputEvent struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}
