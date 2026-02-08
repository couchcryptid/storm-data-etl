package http_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/couchcryptid/storm-data-etl-service/internal/adapter/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReadiness struct {
	ready bool
}

func (m *mockReadiness) Ready() bool { return m.ready }

func newTestServer(ready bool) *httpadapter.Server {
	return httpadapter.NewServer(":0", &mockReadiness{ready: ready}, slog.Default())
}

func TestHealthzReturns200(t *testing.T) {
	srv := newTestServer(true)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "healthy", body["status"])
}

func TestReadyzReturns200WhenReady(t *testing.T) {
	srv := newTestServer(true)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ready", body["status"])
}

func TestReadyzReturns503WhenNotReady(t *testing.T) {
	srv := newTestServer(false)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "not_ready", body["status"])
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newTestServer(true)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "go_goroutines")
}
