package business

import (
	"github.com/gin-gonic/gin"
)

type AuthStrategy interface {
	AuthFunc() gin.HandlerFunc
}

type AuthOperator struct {
	strategy AuthStrategy
	username string
	isAdmin  string
}

func (operator *AuthOperator) SetUsername(name string) {
	operator.username = name
}

func (operator *AuthOperator) GetUsername() string {
	return operator.username
}

func (operator *AuthOperator) SetAuthStrategy(authStrategy AuthStrategy) {
	operator.strategy = authStrategy
}

func (operator *AuthOperator) AuthFunc() gin.HandlerFunc {
	return operator.strategy.AuthFunc()
}
