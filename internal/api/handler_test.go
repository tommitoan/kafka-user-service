package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"kafka-user-service/internal/api"
	"kafka-user-service/internal/models"
	"kafka-user-service/internal/service"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// ── Mock UserService ──────────────────────────────────────────────────────────

type mockUserService struct {
	mock.Mock
}

func (m *mockUserService) Create(ctx context.Context, req service.CreateUserRequest) (*models.User, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

func (m *mockUserService) GetByID(ctx context.Context, id uuid.UUID) (*models.User, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

func (m *mockUserService) List(ctx context.Context, offset, limit int) (*service.ListUsersResponse, error) {
	args := m.Called(ctx, offset, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*service.ListUsersResponse), args.Error(1)
}

func (m *mockUserService) Update(ctx context.Context, id uuid.UUID, req service.UpdateUserRequest) (*models.User, error) {
	args := m.Called(ctx, id, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

func (m *mockUserService) Delete(ctx context.Context, id uuid.UUID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func setupRouter(svc service.UserService) *gin.Engine {
	r := gin.New()
	api.NewUserHandler(svc).RegisterRoutes(r)
	return r
}

func sampleUser() *models.User {
	return &models.User{
		ID:        uuid.New(),
		Name:      "Alice",
		Email:     "alice@example.com",
		Age:       30,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func toJSON(t *testing.T, v any) *bytes.Buffer {
	t.Helper()
	b, err := json.Marshal(v)
	assert.NoError(t, err)
	return bytes.NewBuffer(b)
}

// ── POST /api/v1/users ────────────────────────────────────────────────────────

func TestCreateUser_201(t *testing.T) {
	svc := &mockUserService{}
	u := sampleUser()
	svc.On("Create", mock.Anything, mock.AnythingOfType("service.CreateUserRequest")).Return(u, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/users", toJSON(t, map[string]any{
		"name": "Alice", "email": "alice@example.com", "age": 30,
	}))
	req.Header.Set("Content-Type", "application/json")

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	var got models.User
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "Alice", got.Name)
}

func TestCreateUser_400_MissingFields(t *testing.T) {
	svc := &mockUserService{}

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/users", toJSON(t, map[string]any{}))
	req.Header.Set("Content-Type", "application/json")

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	svc.AssertNotCalled(t, "Create")
}

// ── GET /api/v1/users/:id ─────────────────────────────────────────────────────

func TestGetUser_200(t *testing.T) {
	svc := &mockUserService{}
	u := sampleUser()
	svc.On("GetByID", mock.Anything, u.ID).Return(u, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/users/"+u.ID.String(), nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetUser_404(t *testing.T) {
	svc := &mockUserService{}
	id := uuid.New()
	svc.On("GetByID", mock.Anything, id).Return(nil, service.ErrNotFound)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/users/"+id.String(), nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetUser_400_InvalidUUID(t *testing.T) {
	svc := &mockUserService{}

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/users/not-a-uuid", nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ── GET /api/v1/users ─────────────────────────────────────────────────────────

func TestListUsers_200(t *testing.T) {
	svc := &mockUserService{}
	resp := &service.ListUsersResponse{
		Users:  []*models.User{sampleUser()},
		Total:  1,
		Offset: 0,
		Limit:  20,
	}
	svc.On("List", mock.Anything, 0, 20).Return(resp, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/users", nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var got service.ListUsersResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, int64(1), got.Total)
}

// ── PUT /api/v1/users/:id ────────────────────────────────────────────────────

func TestUpdateUser_200(t *testing.T) {
	svc := &mockUserService{}
	u := sampleUser()
	u.Name = "Alice Updated"
	svc.On("Update", mock.Anything, u.ID, mock.AnythingOfType("service.UpdateUserRequest")).Return(u, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/users/"+u.ID.String(), toJSON(t, map[string]any{"name": "Alice Updated"}))
	req.Header.Set("Content-Type", "application/json")

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestUpdateUser_404(t *testing.T) {
	svc := &mockUserService{}
	id := uuid.New()
	svc.On("Update", mock.Anything, id, mock.Anything).Return(nil, service.ErrNotFound)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/users/"+id.String(), toJSON(t, map[string]any{"name": "X"}))
	req.Header.Set("Content-Type", "application/json")

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// ── DELETE /api/v1/users/:id ──────────────────────────────────────────────────

func TestDeleteUser_204(t *testing.T) {
	svc := &mockUserService{}
	id := uuid.New()
	svc.On("Delete", mock.Anything, id).Return(nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/users/"+id.String(), nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestDeleteUser_404(t *testing.T) {
	svc := &mockUserService{}
	id := uuid.New()
	svc.On("Delete", mock.Anything, id).Return(service.ErrNotFound)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/users/"+id.String(), nil)

	setupRouter(svc).ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
