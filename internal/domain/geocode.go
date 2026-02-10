package domain

import (
	"context"
	"log/slog"
)

// EnrichWithGeocoding attempts to enrich an event with geocoding data.
// If geocoder is nil or geocoding fails, the event is returned with
// Geocoding.Source set accordingly (graceful degradation).
func EnrichWithGeocoding(ctx context.Context, event StormEvent, geocoder Geocoder, logger *slog.Logger) StormEvent {
	if geocoder == nil {
		return event
	}

	// Zero coordinates indicate missing data, not the (0,0) "null island" point
	// in the Gulf of Guinea — US storm events never occur there.
	hasCoords := event.Geo.Lat != 0 || event.Geo.Lon != 0
	hasName := event.Location.Name != "" && event.Location.State != ""

	// Forward geocode: location name → coordinates (when coords are missing).
	if !hasCoords && hasName {
		result, err := geocoder.ForwardGeocode(ctx, event.Location.Name, event.Location.State)
		if err != nil {
			logger.Warn("forward geocoding failed",
				"event_id", event.ID,
				"location", event.Location.Name,
				"state", event.Location.State,
				"error", err,
			)
			event.Geocoding.Source = "failed"
			return event
		}
		if result.Lat != 0 || result.Lon != 0 {
			event.Geo.Lat = result.Lat
			event.Geo.Lon = result.Lon
			event.Geocoding.FormattedAddress = result.FormattedAddress
			event.Geocoding.PlaceName = result.PlaceName
			event.Geocoding.Confidence = result.Confidence
			event.Geocoding.Source = "forward"
			return event
		}
		event.Geocoding.Source = "original"
		return event
	}

	// Reverse geocode: coordinates → place details (when coords are present).
	if hasCoords {
		result, err := geocoder.ReverseGeocode(ctx, event.Geo.Lat, event.Geo.Lon)
		if err != nil {
			logger.Warn("reverse geocoding failed",
				"event_id", event.ID,
				"lat", event.Geo.Lat,
				"lon", event.Geo.Lon,
				"error", err,
			)
			event.Geocoding.Source = "failed"
			return event
		}
		if result.FormattedAddress != "" {
			event.Geocoding.FormattedAddress = result.FormattedAddress
			event.Geocoding.PlaceName = result.PlaceName
			event.Geocoding.Confidence = result.Confidence
			event.Geocoding.Source = "reverse"
			return event
		}
		event.Geocoding.Source = "original"
		return event
	}

	event.Geocoding.Source = "original"
	return event
}
