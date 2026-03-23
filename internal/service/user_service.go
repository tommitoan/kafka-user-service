package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"kafka-user-service/internal/kafka"
	"kafka-user-service/internal/models"
	"kafka-user-service/internal/repository"
)

type CreateUserRequest struct {
	Name  string `json:"name"  binding:"required"`
	Email string `json:"email" binding:"required,email"`
	Age   int    `json:"age"   binding:"gte=0,lte=150"`
}

type UpdateUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email" binding:"omitempty,email"`
	Age   int    `json:"age"   binding:"omitempty,gte=0,lte=150"`
}

type ListUsersResponse struct {
	Users  []*models.User `json:"users"`
	Total  int64          `json:"total"`
	Offset int            `json:"offset"`
	Limit  int            `json:"limit"`
}

//go:generate mockery --name=UserService --output=../mocks --outpkg=mocks
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
	user := &models.User{
		Name:  req.Name,
		Email: req.Email,
		Age:   req.Age,
	}

	if err := s.repo.Create(ctx, user); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	s.publishEvent(ctx, user, models.EventCreated)
	return user, nil
}

func (s *userService) GetByID(ctx context.Context, id uuid.UUID) (*models.User, error) {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get user: %w", err)
	}
	return user, nil
}

func (s *userService) List(ctx context.Context, offset, limit int) (*ListUsersResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	users, total, err := s.repo.List(ctx, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	return &ListUsersResponse{
		Users:  users,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	}, nil
}

func (s *userService) Update(ctx context.Context, id uuid.UUID, req UpdateUserRequest) (*models.User, error) {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get user for update: %w", err)
	}

	if req.Name != "" {
		user.Name = req.Name
	}
	if req.Email != "" {
		user.Email = req.Email
	}
	if req.Age != 0 {
		user.Age = req.Age
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
		if err == gorm.ErrRecordNotFound {
			return ErrNotFound
		}
		return fmt.Errorf("get user for delete: %w", err)
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete user: %w", err)
	}

	s.publishEvent(ctx, user, models.EventDeleted)
	return nil
}

func (s *userService) publishEvent(ctx context.Context, user *models.User, evtType models.EventType) {
	event := &models.UserEvent{
		EventType: string(evtType),
		UserID:    user.ID.String(),
		Name:      user.Name,
		Email:     user.Email,
		Age:       user.Age,
		Timestamp: time.Now().UTC(),
	}
	// Fire-and-forget; log errors in real apps
	_ = s.producer.PublishUserEvent(ctx, event)
}

var ErrNotFound = fmt.Errorf("not found")
