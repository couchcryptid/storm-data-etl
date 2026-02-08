package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRawEvent(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		payload := StormEvent{
			ID:        "evt-123",
			EventType: "hail",
			Magnitude: 1.5,
			Unit:      "in",
			Source:    "COOP",
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		raw := RawEvent{Value: data}
		result, err := ParseRawEvent(raw)

		require.NoError(t, err)
		assert.Equal(t, "evt-123", result.ID)
		assert.Equal(t, "hail", result.EventType)
		assert.Equal(t, 1.5, result.Magnitude)
		assert.Equal(t, "in", result.Unit)
		assert.Equal(t, "COOP", result.Source)
		assert.Equal(t, data, result.RawPayload)
		assert.True(t, result.ProcessedAt.IsZero())
	})

	t.Run("invalid JSON", func(t *testing.T) {
		raw := RawEvent{Value: []byte("{invalid json")}
		_, err := ParseRawEvent(raw)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse raw event")
	})

	t.Run("empty JSON", func(t *testing.T) {
		raw := RawEvent{Value: []byte("{}")}
		result, err := ParseRawEvent(raw)

		require.NoError(t, err)
		assert.Equal(t, "", result.ID)
		assert.True(t, result.ProcessedAt.IsZero())
	})
}

func TestEnrichStormEvent(t *testing.T) {
	fixedTime := time.Date(2024, 4, 26, 12, 30, 45, 0, time.UTC)
	mockClock := clockwork.NewFakeClockAt(fixedTime)
	SetClock(mockClock)
	defer SetClock(nil)

	t.Run("hail event with location", func(t *testing.T) {
		event := StormEvent{
			ID:        "evt-1",
			EventType: "hail",
			Magnitude: 175,
			Unit:      "in",
			BeginTime: time.Date(2024, 4, 26, 15, 45, 0, 0, time.UTC),
			Comments:  "Large hail reported (ABC)",
			Location:  Location{Raw: "5.2 NW AUSTIN"},
		}

		result := EnrichStormEvent(event)

		assert.Equal(t, "hail", result.EventType)
		assert.Equal(t, "in", result.Unit)
		assert.Equal(t, 1.75, result.Magnitude) // normalized from 175
		assert.Equal(t, "severe", result.Severity)
		assert.Equal(t, "ABC", result.SourceOffice)
		assert.Equal(t, "AUSTIN", result.Location.Name)
		assert.Equal(t, 5.2, result.Location.Distance)
		assert.Equal(t, "NW", result.Location.Direction)
		assert.Equal(t, "2024-04-26T15:00:00Z", result.TimeBucket)
		assert.Equal(t, fixedTime, result.ProcessedAt)
	})

	t.Run("wind event", func(t *testing.T) {
		event := StormEvent{
			EventType: "wind",
			Magnitude: 85,
		}

		result := EnrichStormEvent(event)

		assert.Equal(t, "wind", result.EventType)
		assert.Equal(t, "mph", result.Unit)
		assert.Equal(t, 85.0, result.Magnitude)
		assert.Equal(t, "severe", result.Severity)
	})

	t.Run("tornado event", func(t *testing.T) {
		event := StormEvent{
			EventType: "tornado",
			Magnitude: 3,
		}

		result := EnrichStormEvent(event)

		assert.Equal(t, "tornado", result.EventType)
		assert.Equal(t, "f_scale", result.Unit)
		assert.Equal(t, 3.0, result.Magnitude)
		assert.Equal(t, "severe", result.Severity)
	})
}

func TestNormalizeEventType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"hail", "hail", "hail"},
		{"wind", "wind", "wind"},
		{"tornado", "tornado", "tornado"},
		{"torn rejected", "torn", ""},
		{"uppercase rejected", "HAIL", ""},
		{"mixed case rejected", "Hail", ""},
		{"with spaces rejected", "  hail  ", ""},
		{"uppercase wind rejected", "WIND", ""},
		{"uppercase tornado rejected", "TORNADO", ""},
		{"unknown type", "snow", ""},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeEventType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeUnit(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		unit      string
		expected  string
	}{
		{"explicit unit", "hail", "cm", "cm"},
		{"explicit unit with spaces", "hail", "  in  ", "in"},
		{"hail default", "hail", "", "in"},
		{"wind default", "wind", "", "mph"},
		{"tornado default", "tornado", "", "f_scale"},
		{"unknown type", "earthquake", "", ""},
		{"empty type and unit", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeUnit(tt.eventType, tt.unit)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeMagnitude(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		magnitude float64
		unit      string
		expected  float64
	}{
		{"hail conversion from hundredths", "hail", 175, "in", 1.75},
		{"hail conversion from hundredths large", "hail", 250, "in", 2.5},
		{"hail already in inches", "hail", 1.5, "in", 1.5},
		{"hail in cm", "hail", 5.0, "cm", 5.0},
		{"wind no conversion", "wind", 85, "mph", 85},
		{"tornado no conversion", "tornado", 3, "f_scale", 3},
		{"zero magnitude", "hail", 0, "in", 0},
		{"unknown type", "snow", 100, "in", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeMagnitude(tt.eventType, tt.magnitude, tt.unit)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeriveSeverity(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		magnitude float64
		expected  string
	}{
		// Hail
		{"hail minor", "hail", 0.5, "minor"},
		{"hail moderate", "hail", 1.0, "moderate"},
		{"hail severe", "hail", 2.0, "severe"},
		{"hail extreme", "hail", 3.0, "extreme"},
		{"hail edge case 0.75", "hail", 0.75, "moderate"},
		{"hail edge case 1.5", "hail", 1.5, "severe"},
		{"hail edge case 2.5", "hail", 2.5, "extreme"},

		// Wind
		{"wind minor", "wind", 45, "minor"},
		{"wind moderate", "wind", 60, "moderate"},
		{"wind severe", "wind", 85, "severe"},
		{"wind extreme", "wind", 100, "extreme"},
		{"wind edge case 50", "wind", 50, "moderate"},
		{"wind edge case 74", "wind", 74, "severe"},
		{"wind edge case 96", "wind", 96, "extreme"},

		// Tornado
		{"tornado minor F1", "tornado", 1, "minor"},
		{"tornado moderate F2", "tornado", 2, "moderate"},
		{"tornado severe F3", "tornado", 3, "severe"},
		{"tornado severe F4", "tornado", 4, "severe"},
		{"tornado extreme F5", "tornado", 5, "extreme"},

		// Edge cases
		{"zero magnitude", "hail", 0, ""},
		{"unknown type", "earthquake", 5.5, ""},
		{"empty type", "", 100, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deriveSeverity(tt.eventType, tt.magnitude)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractSourceOffice(t *testing.T) {
	tests := []struct {
		name     string
		comments string
		expected string
	}{
		{"valid 3 letter code", "Storm reported by spotter (ABC)", "ABC"},
		{"valid 4 letter code", "Heavy rain observed (ABCD)", "ABCD"},
		{"valid 5 letter code", "Report from radar (ABCDE)", "ABCDE"},
		{"no code", "Storm reported", ""},
		{"empty comments", "", ""},
		{"lowercase not matched", "storm (abc)", ""},
		{"code not at end", "(ABC) storm reported", ""},
		{"multiple parentheses", "Storm (ABC) test (DEF)", "DEF"},
		{"space inside parentheses not matched", "Storm (ABC )  ", ""},
		{"only digits in parentheses", "Storm (123)", ""},
		{"mixed alphanumeric", "Storm (AB12)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSourceOffice(tt.comments)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseLocation(t *testing.T) {
	tests := []struct {
		name              string
		location          string
		expectedName      string
		expectedDistance  float64
		expectedDirection string
	}{
		{"valid N direction", "5 N AUSTIN", "AUSTIN", 5.0, "N"},
		{"valid NW direction", "5.2 NW AUSTIN", "AUSTIN", 5.2, "NW"},
		{"valid NNE direction", "10.5 NNE SAN ANTONIO", "SAN ANTONIO", 10.5, "NNE"},
		{"valid with city name", "3.7 SW HOUSTON", "HOUSTON", 3.7, "SW"},
		{"decimal distance", "2.25 E DALLAS", "DALLAS", 2.25, "E"},
		{"no match - missing direction", "5 AUSTIN", "5 AUSTIN", 0, ""},
		{"no match - missing distance", "N AUSTIN", "N AUSTIN", 0, ""},
		{"no match - just city", "AUSTIN", "AUSTIN", 0, ""},
		{"empty string", "", "", 0, ""},
		{"spaces only", "   ", "", 0, ""},
		{"malformed distance", "abc N AUSTIN", "abc N AUSTIN", 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, distance, direction := parseLocation(tt.location)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedDistance, distance)
			assert.Equal(t, tt.expectedDirection, direction)
		})
	}
}

func TestDeriveTimeBucket(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			"hour boundary",
			time.Date(2024, 4, 26, 15, 0, 0, 0, time.UTC),
			"2024-04-26T15:00:00Z",
		},
		{
			"truncate to hour",
			time.Date(2024, 4, 26, 15, 45, 30, 500, time.UTC),
			"2024-04-26T15:00:00Z",
		},
		{
			"different timezone",
			time.Date(2024, 4, 26, 15, 30, 0, 0, time.FixedZone("EST", -5*3600)),
			"2024-04-26T20:00:00Z",
		},
		{
			"zero time",
			time.Time{},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deriveTimeBucket(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSerializeStormEvent(t *testing.T) {
	fixedTime := time.Date(2024, 4, 26, 12, 0, 0, 0, time.UTC)

	t.Run("successful serialization", func(t *testing.T) {
		event := StormEvent{
			ID:          "evt-123",
			EventType:   "hail",
			Magnitude:   1.5,
			Unit:        "in",
			Severity:    "moderate",
			ProcessedAt: fixedTime,
		}

		result, err := SerializeStormEvent(event)

		require.NoError(t, err)
		assert.Equal(t, []byte("evt-123"), result.Key)

		var unmarshaled StormEvent
		err = json.Unmarshal(result.Value, &unmarshaled)
		require.NoError(t, err)
		assert.Equal(t, "evt-123", unmarshaled.ID)
		assert.Equal(t, "hail", unmarshaled.EventType)
		assert.Equal(t, 1.5, unmarshaled.Magnitude)

		assert.Equal(t, "hail", result.Headers["type"])
		assert.Equal(t, "2024-04-26T12:00:00Z", result.Headers["processed_at"])
	})

	t.Run("empty event ID", func(t *testing.T) {
		event := StormEvent{
			EventType:   "wind",
			ProcessedAt: fixedTime,
		}

		result, err := SerializeStormEvent(event)

		require.NoError(t, err)
		assert.Empty(t, result.Key)
		assert.Equal(t, "wind", result.Headers["type"])
	})

	t.Run("complex nested structures", func(t *testing.T) {
		event := StormEvent{
			ID:        "evt-456",
			EventType: "tornado",
			Geo: Geo{
				Lat: 30.2672,
				Lon: -97.7431,
			},
			Location: Location{
				Raw:       "5.2 NW AUSTIN",
				Name:      "AUSTIN",
				Distance:  5.2,
				Direction: "NW",
				State:     "TX",
				County:    "TRAVIS",
			},
			BeginTime:    time.Date(2024, 4, 26, 15, 0, 0, 0, time.UTC),
			EndTime:      time.Date(2024, 4, 26, 15, 30, 0, 0, time.UTC),
			Comments:     "Tornado confirmed (AUS)",
			SourceOffice: "AUS",
			ProcessedAt:  fixedTime,
		}

		result, err := SerializeStormEvent(event)

		require.NoError(t, err)

		var unmarshaled StormEvent
		err = json.Unmarshal(result.Value, &unmarshaled)
		require.NoError(t, err)
		assert.Equal(t, 30.2672, unmarshaled.Geo.Lat)
		assert.Equal(t, -97.7431, unmarshaled.Geo.Lon)
		assert.Equal(t, "AUSTIN", unmarshaled.Location.Name)
		assert.Equal(t, "AUS", unmarshaled.SourceOffice)
	})
}

func TestSetClock(t *testing.T) {
	t.Run("set custom clock", func(t *testing.T) {
		fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		mockClock := clockwork.NewFakeClockAt(fixedTime)

		SetClock(mockClock)
		assert.Equal(t, fixedTime, clock.Now())

		SetClock(nil) // reset
	})

	t.Run("reset to real clock", func(t *testing.T) {
		fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		mockClock := clockwork.NewFakeClockAt(fixedTime)

		SetClock(mockClock)
		SetClock(nil)

		// Real clock should return current time (within a small window)
		now := clock.Now()
		assert.True(t, time.Since(now) < time.Second)
	})
}
