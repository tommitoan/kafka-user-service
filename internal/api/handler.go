package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"kafka-user-service/internal/service"
)

type UserHandler struct {
	svc service.UserService
}

func NewUserHandler(svc service.UserService) *UserHandler {
	return &UserHandler{svc: svc}
}

func (h *UserHandler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	{
		users := v1.Group("/users")
		{
			users.POST("", h.CreateUser)
			users.GET("", h.ListUsers)
			users.GET("/:id", h.GetUser)
			users.PUT("/:id", h.UpdateUser)
			users.DELETE("/:id", h.DeleteUser)
		}
	}

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
}

// CreateUser godoc
// @Summary      Create a user
// @Description  Create a new user and publish a CREATED event to Kafka
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        user  body      service.CreateUserRequest  true  "User payload"
// @Success      201   {object}  models.User
// @Failure      400   {object}  map[string]string
// @Failure      500   {object}  map[string]string
// @Router       /users [post]
func (h *UserHandler) CreateUser(c *gin.Context) {
	var req service.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	user, err := h.svc.Create(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusCreated, user)
}

// GetUser godoc
// @Summary      Get a user
// @Description  Get a single user by UUID
// @Tags         users
// @Produce      json
// @Param        id   path      string  true  "User UUID"
// @Success      200  {object}  models.User
// @Failure      400  {object}  map[string]string
// @Failure      404  {object}  map[string]string
// @Router       /users/{id} [get]
func (h *UserHandler) GetUser(c *gin.Context) {
	id, err := parseUUID(c, "id")
	if err != nil {
		return
	}

	user, err := h.svc.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			c.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusOK, user)
}

// ListUsers godoc
// @Summary      List users
// @Description  Get a paginated list of users
// @Tags         users
// @Produce      json
// @Param        offset  query     int  false  "Offset"  default(0)
// @Param        limit   query     int  false  "Limit"   default(20)
// @Success      200     {object}  service.ListUsersResponse
// @Failure      500     {object}  map[string]string
// @Router       /users [get]
func (h *UserHandler) ListUsers(c *gin.Context) {
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))

	resp, err := h.svc.List(c.Request.Context(), offset, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusOK, resp)
}

// UpdateUser godoc
// @Summary      Update a user
// @Description  Update an existing user and publish an UPDATED event to Kafka
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        id    path      string                     true  "User UUID"
// @Param        user  body      service.UpdateUserRequest  true  "Fields to update"
// @Success      200   {object}  models.User
// @Failure      400   {object}  map[string]string
// @Failure      404   {object}  map[string]string
// @Failure      500   {object}  map[string]string
// @Router       /users/{id} [put]
func (h *UserHandler) UpdateUser(c *gin.Context) {
	id, err := parseUUID(c, "id")
	if err != nil {
		return
	}

	var req service.UpdateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	user, err := h.svc.Update(c.Request.Context(), id, req)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			c.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusOK, user)
}

// DeleteUser godoc
// @Summary      Delete a user
// @Description  Soft-delete a user and publish a DELETED event to Kafka
// @Tags         users
// @Param        id  path  string  true  "User UUID"
// @Success      204
// @Failure      404  {object}  map[string]string
// @Failure      500  {object}  map[string]string
// @Router       /users/{id} [delete]
func (h *UserHandler) DeleteUser(c *gin.Context) {
	id, err := parseUUID(c, "id")
	if err != nil {
		return
	}

	if err := h.svc.Delete(c.Request.Context(), id); err != nil {
		if errors.Is(err, service.ErrNotFound) {
			c.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusNoContent, nil)
}

// --- helpers ---

func parseUUID(c *gin.Context, param string) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param(param))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid UUID"})
		return uuid.Nil, err
	}
	return id, nil
}

func errorResponse(err error) gin.H {
	return gin.H{"error": err.Error()}
}
