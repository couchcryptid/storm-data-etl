package mapbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
)

// Client implements domain.Geocoder using the Mapbox Geocoding API.
type Client struct {
	token      string
	httpClient *http.Client
	baseURL    string
	metrics    *observability.Metrics
	logger     *slog.Logger
}

// NewClient creates a Mapbox geocoding client.
func NewClient(token string, timeout time.Duration, metrics *observability.Metrics, logger *slog.Logger) *Client {
	return &Client{
		token: token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		baseURL: "https://api.mapbox.com/geocoding/v5/mapbox.places",
		metrics: metrics,
		logger:  logger,
	}
}

// ForwardGeocode converts a location name and state to coordinates.
func (c *Client) ForwardGeocode(ctx context.Context, name, state string) (domain.GeocodingResult, error) {
	query := name
	if state != "" {
		query = fmt.Sprintf("%s, %s", name, state)
	}

	u := fmt.Sprintf("%s/%s.json", c.baseURL, url.PathEscape(query))
	params := url.Values{
		"access_token": {c.token},
		"limit":        {"1"},
		"types":        {"place,locality"}, // restrict to populated places (excludes roads, waterways, etc.)
	}

	return c.doRequest(ctx, u+"?"+params.Encode(), "forward")
}

// ReverseGeocode converts coordinates to place details.
func (c *Client) ReverseGeocode(ctx context.Context, lat, lon float64) (domain.GeocodingResult, error) {
	// Mapbox uses lon,lat order.
	coord := fmt.Sprintf("%.6f,%.6f", lon, lat)
	u := fmt.Sprintf("%s/%s.json", c.baseURL, coord)
	params := url.Values{
		"access_token": {c.token},
		"limit":        {"1"},
	}

	return c.doRequest(ctx, u+"?"+params.Encode(), "reverse")
}

func (c *Client) doRequest(ctx context.Context, fullURL, source string) (domain.GeocodingResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		c.metrics.GeocodeRequests.WithLabelValues(source, "error").Inc()
		return domain.GeocodingResult{}, fmt.Errorf("create request: %w", err)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	c.metrics.GeocodeAPIDuration.WithLabelValues(source).Observe(time.Since(start).Seconds())

	if err != nil {
		c.metrics.GeocodeRequests.WithLabelValues(source, "error").Inc()
		return domain.GeocodingResult{}, fmt.Errorf("%s geocode request: %w", source, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.metrics.GeocodeRequests.WithLabelValues(source, "error").Inc()
		return domain.GeocodingResult{}, fmt.Errorf("mapbox API error: status %d: %s", resp.StatusCode, body)
	}

	var mapboxResp response
	if err := json.NewDecoder(resp.Body).Decode(&mapboxResp); err != nil {
		c.metrics.GeocodeRequests.WithLabelValues(source, "error").Inc()
		return domain.GeocodingResult{}, fmt.Errorf("decode response: %w", err)
	}

	if len(mapboxResp.Features) == 0 {
		c.metrics.GeocodeRequests.WithLabelValues(source, "empty").Inc()
		return domain.GeocodingResult{}, nil
	}

	c.metrics.GeocodeRequests.WithLabelValues(source, "success").Inc()
	f := mapboxResp.Features[0]
	result := domain.GeocodingResult{
		FormattedAddress: f.PlaceName,
		PlaceName:        f.Text,
		Confidence:       f.Relevance,
	}
	if len(f.Center) == 2 {
		result.Lon = f.Center[0]
		result.Lat = f.Center[1]
	}
	return result, nil
}

// Mapbox API response types.

type response struct {
	Features []feature `json:"features"`
}

type feature struct {
	Center    []float64 `json:"center"` // [lon, lat]
	PlaceName string    `json:"place_name"`
	Text      string    `json:"text"`
	Relevance float64   `json:"relevance"`
}
