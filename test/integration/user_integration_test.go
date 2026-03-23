//go:build integration

// Run with: go test -tags=integration -v ./test/integration/...
//
// No Docker required. Uses embedded Postgres and a mock Kafka producer.

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"kafka-user-service/internal/api"
	"kafka-user-service/internal/db"
	"kafka-user-service/internal/mocks"
	"kafka-user-service/internal/models"
	"kafka-user-service/internal/repository"
	"kafka-user-service/internal/service"
)

// IntegrationSuite uses embedded Postgres + mock Kafka producer.
type IntegrationSuite struct {
	suite.Suite
	pg       *embeddedpostgres.EmbeddedPostgres
	gormDB   *gorm.DB
	router   *gin.Engine
	producer *mocks.MockProducer
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationSuite))
}

// SetupSuite starts embedded Postgres once for the whole suite.
func (s *IntegrationSuite) SetupSuite() {
	s.pg = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("userdb").
		Port(15432), // Use a non-standard port to avoid conflicts
	)
	require.NoError(s.T(), s.pg.Start())

	dsn := "host=localhost port=15432 user=postgres password=postgres dbname=userdb sslmode=disable"
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(s.T(), err)
	s.gormDB = gormDB

	// Run migrations
	require.NoError(s.T(), db.AutoMigrate(gormDB))

	gin.SetMode(gin.TestMode)
	s.setupRouter()
}

// TearDownSuite stops embedded Postgres after all tests finish.
func (s *IntegrationSuite) TearDownSuite() {
	if s.pg != nil {
		_ = s.pg.Stop()
	}
}

// SetupTest resets mock and truncates users table before each test.
func (s *IntegrationSuite) SetupTest() {
	s.producer = &mocks.MockProducer{}
	s.setupRouter()
	s.gormDB.Exec("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
}

func (s *IntegrationSuite) setupRouter() {
	userRepo := repository.NewUserRepository(s.gormDB)
	userSvc := service.NewUserService(userRepo, s.producer)
	r := gin.New()
	api.NewUserHandler(userSvc).RegisterRoutes(r)
	s.router = r
}

// ── helpers ───────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) post(path string, body any) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	s.router.ServeHTTP(w, req)
	return w
}

func (s *IntegrationSuite) get(path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, path, nil)
	s.router.ServeHTTP(w, req)
	return w
}

func (s *IntegrationSuite) put(path string, body any) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	s.router.ServeHTTP(w, req)
	return w
}

func (s *IntegrationSuite) delete(path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, path, nil)
	s.router.ServeHTTP(w, req)
	return w
}

func (s *IntegrationSuite) expectEvent(eventType string) {
	s.producer.On("PublishUserEvent", mock.Anything,
		mock.MatchedBy(func(e *models.UserEvent) bool {
			return e.EventType == eventType
		}),
	).Return(nil).Once()
}

// ── Create ────────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) TestCreateUser_Success() {
	s.expectEvent("CREATED")

	w := s.post("/api/v1/users", map[string]any{
		"name": "Alice", "email": "alice@example.com", "age": 30,
	})

	assert.Equal(s.T(), http.StatusCreated, w.Code)

	var user models.User
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &user))
	assert.NotEmpty(s.T(), user.ID)
	assert.Equal(s.T(), "Alice", user.Name)
	assert.Equal(s.T(), "alice@example.com", user.Email)
	assert.Equal(s.T(), 30, user.Age)

	s.producer.AssertExpectations(s.T())
}

func (s *IntegrationSuite) TestCreateUser_DuplicateEmail() {
	s.expectEvent("CREATED")
	s.post("/api/v1/users", map[string]any{"name": "Alice", "email": "dup@example.com", "age": 25})

	// Second create with same email — no event should fire
	w := s.post("/api/v1/users", map[string]any{"name": "Alice2", "email": "dup@example.com", "age": 26})
	assert.Equal(s.T(), http.StatusInternalServerError, w.Code)

	s.producer.AssertExpectations(s.T())
}

func (s *IntegrationSuite) TestCreateUser_MissingName() {
	w := s.post("/api/v1/users", map[string]any{"email": "no-name@example.com"})
	assert.Equal(s.T(), http.StatusBadRequest, w.Code)
	s.producer.AssertNotCalled(s.T(), "PublishUserEvent")
}

// ── GetByID ───────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) TestGetUser_Success() {
	s.expectEvent("CREATED")
	w := s.post("/api/v1/users", map[string]any{"name": "Bob", "email": "bob@example.com", "age": 22})
	require.Equal(s.T(), http.StatusCreated, w.Code)

	var created models.User
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &created))

	w2 := s.get("/api/v1/users/" + created.ID.String())
	assert.Equal(s.T(), http.StatusOK, w2.Code)

	var got models.User
	require.NoError(s.T(), json.Unmarshal(w2.Body.Bytes(), &got))
	assert.Equal(s.T(), created.ID, got.ID)
	assert.Equal(s.T(), "Bob", got.Name)
}

func (s *IntegrationSuite) TestGetUser_NotFound() {
	w := s.get("/api/v1/users/00000000-0000-0000-0000-000000000000")
	assert.Equal(s.T(), http.StatusNotFound, w.Code)
}

func (s *IntegrationSuite) TestGetUser_InvalidUUID() {
	w := s.get("/api/v1/users/not-a-uuid")
	assert.Equal(s.T(), http.StatusBadRequest, w.Code)
}

// ── List ──────────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) TestListUsers_Empty() {
	w := s.get("/api/v1/users")
	assert.Equal(s.T(), http.StatusOK, w.Code)

	var resp service.ListUsersResponse
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(s.T(), int64(0), resp.Total)
	assert.Empty(s.T(), resp.Users)
}

func (s *IntegrationSuite) TestListUsers_Paginated() {
	// Create 3 users
	for i := 0; i < 3; i++ {
		s.expectEvent("CREATED")
		s.post("/api/v1/users", map[string]any{
			"name":  fmt.Sprintf("User %d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"age":   20 + i,
		})
	}

	w := s.get("/api/v1/users?offset=0&limit=2")
	assert.Equal(s.T(), http.StatusOK, w.Code)

	var resp service.ListUsersResponse
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(s.T(), int64(3), resp.Total)
	assert.Len(s.T(), resp.Users, 2)
	assert.Equal(s.T(), 2, resp.Limit)
}

// ── Update ────────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) TestUpdateUser_Success() {
	s.expectEvent("CREATED")
	w := s.post("/api/v1/users", map[string]any{"name": "Charlie", "email": "charlie@example.com", "age": 28})
	require.Equal(s.T(), http.StatusCreated, w.Code)

	var created models.User
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &created))

	s.expectEvent("UPDATED")
	w2 := s.put("/api/v1/users/"+created.ID.String(), map[string]any{"name": "Charlie Updated", "age": 29})
	assert.Equal(s.T(), http.StatusOK, w2.Code)

	var updated models.User
	require.NoError(s.T(), json.Unmarshal(w2.Body.Bytes(), &updated))
	assert.Equal(s.T(), "Charlie Updated", updated.Name)
	assert.Equal(s.T(), 29, updated.Age)
	// Email unchanged
	assert.Equal(s.T(), "charlie@example.com", updated.Email)

	// Verify persisted in DB
	w3 := s.get("/api/v1/users/" + created.ID.String())
	var fetched models.User
	require.NoError(s.T(), json.Unmarshal(w3.Body.Bytes(), &fetched))
	assert.Equal(s.T(), "Charlie Updated", fetched.Name)

	s.producer.AssertExpectations(s.T())
}

func (s *IntegrationSuite) TestUpdateUser_NotFound() {
	w := s.put("/api/v1/users/00000000-0000-0000-0000-000000000000", map[string]any{"name": "X"})
	assert.Equal(s.T(), http.StatusNotFound, w.Code)
	s.producer.AssertNotCalled(s.T(), "PublishUserEvent")
}

// ── Delete ────────────────────────────────────────────────────────────────────

func (s *IntegrationSuite) TestDeleteUser_Success() {
	s.expectEvent("CREATED")
	w := s.post("/api/v1/users", map[string]any{"name": "Dave", "email": "dave@example.com", "age": 35})
	require.Equal(s.T(), http.StatusCreated, w.Code)

	var created models.User
	require.NoError(s.T(), json.Unmarshal(w.Body.Bytes(), &created))

	s.expectEvent("DELETED")
	w2 := s.delete("/api/v1/users/" + created.ID.String())
	assert.Equal(s.T(), http.StatusNoContent, w2.Code)

	// Should be gone
	w3 := s.get("/api/v1/users/" + created.ID.String())
	assert.Equal(s.T(), http.StatusNotFound, w3.Code)

	s.producer.AssertExpectations(s.T())
}

func (s *IntegrationSuite) TestDeleteUser_NotFound() {
	w := s.delete("/api/v1/users/00000000-0000-0000-0000-000000000000")
	assert.Equal(s.T(), http.StatusNotFound, w.Code)
	s.producer.AssertNotCalled(s.T(), "PublishUserEvent")
}

// ── Event payload assertions ──────────────────────────────────────────────────

func (s *IntegrationSuite) TestCreateUser_EventPayload() {
	var capturedEvent *models.UserEvent
	s.producer.On("PublishUserEvent", mock.Anything, mock.AnythingOfType("*models.UserEvent")).
		Run(func(args mock.Arguments) {
			capturedEvent = args.Get(1).(*models.UserEvent)
		}).
		Return(nil).Once()

	w := s.post("/api/v1/users", map[string]any{"name": "Eve", "email": "eve@example.com", "age": 27})
	require.Equal(s.T(), http.StatusCreated, w.Code)

	require.NotNil(s.T(), capturedEvent)
	assert.Equal(s.T(), "CREATED", capturedEvent.EventType)
	assert.Equal(s.T(), "Eve", capturedEvent.Name)
	assert.Equal(s.T(), "eve@example.com", capturedEvent.Email)
	assert.Equal(s.T(), 27, capturedEvent.Age)
	assert.WithinDuration(s.T(), time.Now(), capturedEvent.Timestamp, 5*time.Second)
	assert.NotEmpty(s.T(), capturedEvent.UserID)

	s.producer.AssertExpectations(s.T())
}
