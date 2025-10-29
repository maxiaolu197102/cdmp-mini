package jwtvalidator

import (
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

// CustomClaims 自定义JWT声明结构（与你的业务令牌字段匹配）
type CustomClaims struct {
	UserID               string `json:"user_id"`  // 业务必需的用户ID
	Username             string `json:"username"` // 可选：用户名
	Role                 string `json:"role"`     // 可选：用户角色
	jwt.RegisteredClaims        // JWT标准声明（包含过期时间等）
	Type                 string `json:"type"`
}

// ValidateToken 校验JWT令牌有效性（完全适配你的withCode错误体系）
// 返回值：
// - 校验通过：返回*CustomClaims和nil
// - 校验失败：返回nil和*errors.withCode类型错误（含业务码和堆栈）
func ValidateToken(tokenString string, jwtKey string) (*CustomClaims, error) {
	var jwtSecret = []byte(jwtKey)
	// 1. 校验令牌为空场景（基础校验，优先处理）
	if tokenString == "" {
		log.Errorf("令牌校验失败：缺少Authorization头")
		return nil, errors.WithCode(
			code.ErrMissingHeader, // 100205
			"请先登录",
		)
	}

	// 2. 处理Bearer前缀并校验格式（避免空令牌进入后续解析）
	tokenString = trimBearerPrefix(tokenString)
	if tokenString == "" {
		log.Errorf("令牌校验失败：授权头格式错误（无有效令牌内容）")
		return nil, errors.WithCode(
			code.ErrInvalidAuthHeader, // 100204
			"invalid authorization header format",
		)
	}

	// 3. 解析令牌并校验签名算法（核心解析逻辑）
	var claims CustomClaims
	token, err := jwt.ParseWithClaims(
		tokenString,
		&claims,
		func(token *jwt.Token) (interface{}, error) {
			// 校验签名算法是否为预期的HMAC类型（防止算法篡改）
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				errMsg := "签名方法不支持: %v"
				log.Errorf(errMsg, token.Header["alg"])
				return nil, errors.WithCode(
					code.ErrTokenInvalid, // 100208（算法不支持属于令牌无效）
					errMsg, token.Header["alg"],
				)
			}
			// 算法合法，返回签名密钥（从配置读取，避免硬编码）
			return jwtSecret, nil
		},
	)

	// 4. 处理解析错误（核心修复：严格按「格式错误→过期错误→签名错误」优先级）
	if err != nil {
		// 打印错误详情，方便后续排查（保留调试日志）
		log.Errorf("解析错误详情：类型=%T, 内容=%+v, 消息=%s", err, err, err.Error())
		errMsg := err.Error() // 统一获取错误消息，避免多次调用

		// 4.1 第一步：优先处理「复合错误」（*jwt.ValidationError）
		if ve, ok := err.(*jwt.ValidationError); ok {
			log.Errorf("复合错误标志位：%v（Expired=%v, Malformed=%v, SignatureInvalid=%v）",
				ve.Errors,
				ve.Errors&jwt.ValidationErrorExpired != 0,
				ve.Errors&jwt.ValidationErrorMalformed != 0,
				ve.Errors&jwt.ValidationErrorSignatureInvalid != 0)

			// 复合错误内优先级：格式错误（Malformed）→ 过期错误 → 签名无效
			// 先处理「畸形令牌」（格式错误，如段数不对）
			if ve.Errors&jwt.ValidationErrorMalformed != 0 {
				log.Errorf("令牌校验失败：格式错误（复合错误）")
				return nil, errors.WithCode(
					code.ErrTokenInvalid, // 100208
					"令牌格式错误",
				)
			}
			// 再处理「过期令牌」（格式正确但已过期）
			if ve.Errors&jwt.ValidationErrorExpired != 0 {
				log.Errorf("令牌校验失败：已过期（复合错误）")
				return nil, errors.WithCode(
					code.ErrExpired, // 100203（与用例预期一致）
					"令牌已过期",
				)
			}
			// 最后处理「复合错误中的签名无效」（格式正确、未过期但签名错）
			if ve.Errors&jwt.ValidationErrorSignatureInvalid != 0 {
				log.Errorf("令牌校验失败：签名无效（复合错误）")
				return nil, errors.WithCode(
					code.ErrSignatureInvalid, // 100202
					"signature is invalid",
				)
			}
		}

		// 4.2 第二步：处理「单一错误」（非复合错误，按优先级排序）
		switch {
		// 先处理「单一过期错误」（部分场景下库直接返回此错误）
		case errors.Is(err, jwt.ErrTokenExpired):
			log.Errorf("令牌校验失败：已过期（单一错误）")
			return nil, errors.WithCode(
				code.ErrExpired,
				"令牌已过期",
			)

		// 再处理「格式错误」（段数不对、Base64解码失败）
		case strings.Contains(errMsg, "invalid number of segments"):
			log.Errorf("令牌校验失败：格式错误（段数无效）")
			return nil, errors.WithCode(
				code.ErrTokenInvalid,
				"令牌格式错误",
			)
		case strings.HasPrefix(errMsg, "illegal base64 data"):
			log.Errorf("令牌校验失败：格式错误（Base64解码失败）")
			return nil, errors.WithCode(
				code.ErrTokenInvalid,
				"令牌格式错误",
			)

		// 后处理「单一签名无效」（格式正确、未过期但签名错）
		case errors.Is(err, jwt.ErrSignatureInvalid):
			log.Errorf("令牌校验失败：签名无效（单一错误）")
			return nil, errors.WithCode(
				code.ErrSignatureInvalid,
				"signature is invalid",
			)

		// 最后处理「其他未分类错误」
		default:
			log.Errorf("令牌校验失败：未分类错误")
			return nil, errors.WithCode(
				code.ErrUnauthorized, // 110003
				"authentication failed: %v", errMsg,
			)
		}
	}

	// 5. 校验令牌有效性并提取声明（解析无错后，确认令牌整体有效）
	if customClaims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		// 业务自定义校验（如角色、用户状态等，已有封装）
		if err := validateCustomClaims(customClaims); err != nil {
			return nil, err
		}
		return customClaims, nil
	}

	// 6. 令牌无效（解析无错但基础校验不通过，如claims类型不匹配）
	log.Errorf("令牌校验失败：无效的令牌内容（claims类型不匹配或令牌未通过基础校验）")
	return nil, errors.WithCode(
		code.ErrTokenInvalid, // 100208
		"token is invalid",
	)
}

// 辅助函数：移除Bearer前缀（兼容大小写）
func trimBearerPrefix(token string) string {
	if len(token) >= 7 && strings.ToLower(token[:7]) == "bearer " {
		return token[7:]
	}
	return token
}

// 辅助函数：业务规则校验（返回withCode错误）
func validateCustomClaims(claims *CustomClaims) error {
	// 校验用户ID是否存在
	if claims.UserID == "" {
		log.Errorf("令牌校验失败：缺少user_id字段")
		return errors.WithCode(
			code.ErrTokenInvalid, // 100208
			"token missing user_id",
		)
	}

	// 校验角色合法性（根据你的业务角色列表调整）
	validRoles := map[string]bool{"user": true, "admin": true, "moderator": true}
	if !validRoles[claims.Role] {
		log.Errorf("令牌校验失败：无效角色 %s", claims.Role)
		return errors.WithCode(
			code.ErrTokenInvalid, // 100208
			"invalid role in token: %s", claims.Role,
		)
	}

	// 校验令牌签发时间（防止过于老旧的令牌）
	if claims.IssuedAt != nil && claims.IssuedAt.Before(time.Now().Add(-7*24*time.Hour)) {
		log.Errorf("令牌校验失败：签发时间过早（超过7天）")
		return errors.WithCode(
			code.ErrExpired, // 100203（视为过期类错误）
			"token issued too early (more than 7 days ago)",
		)
	}

	return nil
}
