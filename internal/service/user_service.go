package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/tommitoan/kafka-user-service/internal/kafka"
	"github.com/tommitoan/kafka-user-service/internal/models"
	"github.com/tommitoan/kafka-user-service/internal/repository"
)

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// Errors surfaced to the API layer.
var (
	ErrNotFound   = repository.ErrNotFound
	ErrEmailTaken = repository.ErrEmailTaken
)

type CreateUserRequest struct {
	Name  string `json:"name"  binding:"required"`
	Email string `json:"email" binding:"required,email"`
	Age   int    `json:"age"   binding:"gte=0,lte=150"`
}

// UpdateUserRequest is a partial update: omitted (nil) fields are left unchanged,
// so a field can be set to its zero value, e.g. {"age": 0}.
type UpdateUserRequest struct {
	Name  *string `json:"name"  binding:"omitempty,min=1"`
	Email *string `json:"email" binding:"omitempty,email"`
	Age   *int    `json:"age"   binding:"omitempty,gte=0,lte=150"`
}

type ListUsersResponse struct {
	Users  []*models.User `json:"users"`
	Total  int64          `json:"total"`
	Offset int            `json:"offset"`
	Limit  int            `json:"limit"`
}

type UserService interface {
	Create(ctx context.Context, req CreateUserRequest) (*models.User, error)
	GetByID(ctx context.Context, id uuid.UUID) (*models.User, error)
	List(ctx context.Context, offset, limit int) (*ListUsersResponse, error)
	Update(ctx context.Context, id uuid.UUID, req UpdateUserRequest) (*models.User, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type userService struct {
	repo     repository.UserRepository
	producer kafka.Producer
}

func NewUserService(repo repository.UserRepository, producer kafka.Producer) UserService {
	return &userService{repo: repo, producer: producer}
}

func (s *userService) Create(ctx context.Context, req CreateUserRequest) (*models.User, error) {
	user := &models.User{Name: req.Name, Email: req.Email, Age: req.Age}

	if err := s.repo.Create(ctx, user); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	s.publishEvent(ctx, user, models.EventCreated)
	return user, nil
}

func (s *userService) GetByID(ctx context.Context, id uuid.UUID) (*models.User, error) {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	return user, nil
}

func (s *userService) List(ctx context.Context, offset, limit int) (*ListUsersResponse, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	users, total, err := s.repo.List(ctx, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	return &ListUsersResponse{Users: users, Total: total, Offset: offset, Limit: limit}, nil
}

func (s *userService) Update(ctx context.Context, id uuid.UUID, req UpdateUserRequest) (*models.User, error) {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get user for update: %w", err)
	}

	if req.Name != nil {
		user.Name = *req.Name
	}
	if req.Email != nil {
		user.Email = *req.Email
	}
	if req.Age != nil {
		user.Age = *req.Age
	}

	if err := s.repo.Update(ctx, user); err != nil {
		return nil, fmt.Errorf("update user: %w", err)
	}

	s.publishEvent(ctx, user, models.EventUpdated)
	return user, nil
}

func (s *userService) Delete(ctx context.Context, id uuid.UUID) error {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("get user for delete: %w", err)
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete user: %w", err)
	}

	s.publishEvent(ctx, user, models.EventDeleted)
	return nil
}

// publishEvent emits a user event after the database write has committed.
//
// This is a dual write (DB, then Kafka) without an outbox: if the publish fails
// the change is persisted but no event is sent. The failure is logged, not
// returned, so the HTTP response still reflects the committed state.
func (s *userService) publishEvent(ctx context.Context, user *models.User, evtType models.EventType) {
	event := &models.UserEvent{
		EventID:   uuid.NewString(), // generated once; stable across at-least-once redelivery
		EventType: string(evtType),
		UserID:    user.ID.String(),
		Name:      user.Name,
		Email:     user.Email,
		Age:       user.Age,
		Timestamp: time.Now().UTC(),
	}
	if err := s.producer.PublishUserEvent(ctx, event); err != nil {
		slog.Error("publish user event failed",
			"event_id", event.EventID, "event_type", evtType, "user_id", event.UserID, "error", err)
	}
}
