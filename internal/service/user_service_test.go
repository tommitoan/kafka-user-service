package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"kafka-user-service/internal/mocks"
	"kafka-user-service/internal/models"
	"kafka-user-service/internal/service"
)

func newTestUser() *models.User {
	return &models.User{
		ID:        uuid.New(),
		Name:      "Alice",
		Email:     "alice@example.com",
		Age:       30,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// ── Create ────────────────────────────────────────────────────────────────────

func TestUserService_Create_Success(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	repo.On("Create", mock.Anything, mock.AnythingOfType("*models.User")).Return(nil)
	prod.On("PublishUserEvent", mock.Anything, mock.AnythingOfType("*models.UserEvent")).Return(nil)

	svc := service.NewUserService(repo, prod)
	user, err := svc.Create(context.Background(), service.CreateUserRequest{
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	})

	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, "Alice", user.Name)
	assert.Equal(t, "alice@example.com", user.Email)

	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestUserService_Create_RepoError(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	repo.On("Create", mock.Anything, mock.Anything).Return(assert.AnError)

	svc := service.NewUserService(repo, prod)
	user, err := svc.Create(context.Background(), service.CreateUserRequest{
		Name:  "Alice",
		Email: "alice@example.com",
	})

	assert.Error(t, err)
	assert.Nil(t, user)
	prod.AssertNotCalled(t, "PublishUserEvent")
	repo.AssertExpectations(t)
}

// ── GetByID ───────────────────────────────────────────────────────────────────

func TestUserService_GetByID_Success(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	want := newTestUser()
	repo.On("GetByID", mock.Anything, want.ID).Return(want, nil)

	svc := service.NewUserService(repo, prod)
	got, err := svc.GetByID(context.Background(), want.ID)

	assert.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	repo.AssertExpectations(t)
}

func TestUserService_GetByID_NotFound(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	id := uuid.New()
	repo.On("GetByID", mock.Anything, id).Return(nil, gorm.ErrRecordNotFound)

	svc := service.NewUserService(repo, prod)
	got, err := svc.GetByID(context.Background(), id)

	assert.ErrorIs(t, err, service.ErrNotFound)
	assert.Nil(t, got)
	repo.AssertExpectations(t)
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestUserService_List_Success(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	users := []*models.User{newTestUser(), newTestUser()}
	repo.On("List", mock.Anything, 0, 20).Return(users, int64(2), nil)

	svc := service.NewUserService(repo, prod)
	resp, err := svc.List(context.Background(), 0, 20)

	assert.NoError(t, err)
	assert.Len(t, resp.Users, 2)
	assert.Equal(t, int64(2), resp.Total)
	repo.AssertExpectations(t)
}

func TestUserService_List_DefaultLimit(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	// limit=0 should be coerced to 20
	repo.On("List", mock.Anything, 0, 20).Return([]*models.User{}, int64(0), nil)

	svc := service.NewUserService(repo, prod)
	resp, err := svc.List(context.Background(), 0, 0)

	assert.NoError(t, err)
	assert.Equal(t, 20, resp.Limit)
	repo.AssertExpectations(t)
}

func TestUserService_List_MaxLimit(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	// limit=999 should be capped at 100
	repo.On("List", mock.Anything, 0, 100).Return([]*models.User{}, int64(0), nil)

	svc := service.NewUserService(repo, prod)
	resp, err := svc.List(context.Background(), 0, 999)

	assert.NoError(t, err)
	assert.Equal(t, 100, resp.Limit)
	repo.AssertExpectations(t)
}

// ── Update ────────────────────────────────────────────────────────────────────

func TestUserService_Update_Success(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	existing := newTestUser()
	repo.On("GetByID", mock.Anything, existing.ID).Return(existing, nil)
	repo.On("Update", mock.Anything, mock.AnythingOfType("*models.User")).Return(nil)
	prod.On("PublishUserEvent", mock.Anything, mock.AnythingOfType("*models.UserEvent")).Return(nil)

	svc := service.NewUserService(repo, prod)
	updated, err := svc.Update(context.Background(), existing.ID, service.UpdateUserRequest{
		Name: "Alice Updated",
	})

	assert.NoError(t, err)
	assert.Equal(t, "Alice Updated", updated.Name)
	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestUserService_Update_NotFound(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	id := uuid.New()
	repo.On("GetByID", mock.Anything, id).Return(nil, gorm.ErrRecordNotFound)

	svc := service.NewUserService(repo, prod)
	_, err := svc.Update(context.Background(), id, service.UpdateUserRequest{Name: "X"})

	assert.ErrorIs(t, err, service.ErrNotFound)
	prod.AssertNotCalled(t, "PublishUserEvent")
}

// ── Delete ────────────────────────────────────────────────────────────────────

func TestUserService_Delete_Success(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	u := newTestUser()
	repo.On("GetByID", mock.Anything, u.ID).Return(u, nil)
	repo.On("Delete", mock.Anything, u.ID).Return(nil)
	prod.On("PublishUserEvent", mock.Anything, mock.AnythingOfType("*models.UserEvent")).Return(nil)

	svc := service.NewUserService(repo, prod)
	err := svc.Delete(context.Background(), u.ID)

	assert.NoError(t, err)
	repo.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestUserService_Delete_NotFound(t *testing.T) {
	repo := &mocks.MockUserRepository{}
	prod := &mocks.MockProducer{}

	id := uuid.New()
	repo.On("GetByID", mock.Anything, id).Return(nil, gorm.ErrRecordNotFound)

	svc := service.NewUserService(repo, prod)
	err := svc.Delete(context.Background(), id)

	assert.ErrorIs(t, err, service.ErrNotFound)
	prod.AssertNotCalled(t, "PublishUserEvent")
}
