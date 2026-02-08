package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ParseRawEvent deserializes a RawEvent's value into a StormEvent.
func ParseRawEvent(raw RawEvent) (StormEvent, error) {
	var event StormEvent
	if err := json.Unmarshal(raw.Value, &event); err != nil {
		return StormEvent{}, fmt.Errorf("parse raw event: %w", err)
	}
	event.RawPayload = raw.Value
	event.ProcessedAt = clock.Now()
	return event, nil
}

// EnrichStormEvent applies enrichment logic to a parsed event.
// Currently a passthrough — extend with normalization, classification, etc.
func EnrichStormEvent(event StormEvent) StormEvent {
	event.EventType = normalizeEventType(event.EventType)
	event.Unit = normalizeUnit(event.EventType, event.Unit)
	event.Magnitude = normalizeMagnitude(event.EventType, event.Magnitude, event.Unit)
	event.Severity = deriveSeverity(event.EventType, event.Magnitude)
	event.SourceOffice = extractSourceOffice(event.Comments)
	locationName, locationDistance, locationDirection := parseLocation(event.Location.Raw)
	event.Location.Name = locationName
	event.Location.Distance = locationDistance
	event.Location.Direction = locationDirection
	event.TimeBucket = deriveTimeBucket(event.BeginTime)
	event.ProcessedAt = clock.Now()
	return event
}

// normalizeEventType validates and normalizes the event type metadata added by the upstream service.
// Event type is not part of the original CSV data; it's added when converting CSV to JSON.
// Accepts: "hail", "wind", "tornado", "torn" (converts to "tornado")
func normalizeEventType(value string) string {
	switch value {
	case "hail", "wind", "tornado":
		return value
	case "torn":
		return "tornado"
	default:
		return ""
	}
}

func normalizeUnit(eventType, unit string) string {
	unit = strings.ToLower(strings.TrimSpace(unit))
	if unit != "" {
		return unit
	}

	switch eventType {
	case "hail":
		return "in"
	case "wind":
		return "mph"
	case "tornado":
		return "f_scale"
	default:
		return ""
	}
}

func normalizeMagnitude(eventType string, magnitude float64, unit string) float64 {
	if magnitude == 0 {
		return magnitude
	}
	if eventType == "hail" && unit == "in" && magnitude >= 10 {
		return magnitude / 100.0
	}
	return magnitude
}

func deriveSeverity(eventType string, magnitude float64) string {
	if magnitude == 0 {
		return ""
	}

	switch eventType {
	case "hail":
		switch {
		case magnitude < 0.75:
			return "minor"
		case magnitude < 1.5:
			return "moderate"
		case magnitude < 2.5:
			return "severe"
		default:
			return "extreme"
		}
	case "wind":
		switch {
		case magnitude < 50:
			return "minor"
		case magnitude < 74:
			return "moderate"
		case magnitude < 96:
			return "severe"
		default:
			return "extreme"
		}
	case "tornado":
		switch {
		case magnitude <= 1:
			return "minor"
		case magnitude == 2:
			return "moderate"
		case magnitude <= 4:
			return "severe"
		default:
			return "extreme"
		}
	default:
		return ""
	}
}

func extractSourceOffice(comments string) string {
	comments = strings.TrimSpace(comments)
	if comments == "" {
		return ""
	}

	re := regexp.MustCompile(`\(([A-Z]{3,5})\)\s*$`)
	matches := re.FindStringSubmatch(comments)
	if len(matches) == 2 {
		return matches[1]
	}

	return ""
}

func parseLocation(location string) (string, float64, string) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", 0, ""
	}

	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s+([NSEW]{1,3})\s+(.+)$`)
	matches := re.FindStringSubmatch(location)
	if len(matches) != 4 {
		return location, 0, ""
	}

	distance, err := parseLocationDistance(matches[1])
	if err != nil {
		return location, 0, ""
	}

	return strings.TrimSpace(matches[3]), distance, matches[2]
}

func parseLocationDistance(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

func deriveTimeBucket(begin time.Time) string {
	if begin.IsZero() {
		return ""
	}

	return begin.UTC().Truncate(time.Hour).Format(time.RFC3339)
}

// SerializeStormEvent marshals a StormEvent into an OutputEvent.
func SerializeStormEvent(event StormEvent) (OutputEvent, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return OutputEvent{}, fmt.Errorf("serialize storm event: %w", err)
	}
	return OutputEvent{
		Key:   []byte(event.ID),
		Value: data,
		Headers: map[string]string{
			"type":         event.EventType,
			"processed_at": event.ProcessedAt.Format(time.RFC3339),
		},
	}, nil
}
