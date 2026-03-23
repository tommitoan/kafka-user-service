package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	"kafka-user-service/internal/models"
)

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) PublishUserEvent(ctx context.Context, event *models.UserEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}
