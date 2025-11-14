package create

import (
	"bytes"
	"context"
	"io"

	"github.com/gin-gonic/gin"
)

// HandlerResult 描述创建控制器执行后的结果。
//
// param Entity: 执行过程中传递的实体实例，通常为资源指针。
// param Payload: 构建返回给客户端的响应数据，可为任意结构体或 map。
type HandlerResult[T any] struct {
	Entity  T
	Payload interface{}
}

// HandlerConfig 定义通用创建控制器的钩子集合。
//
// param Name: 创建流程名称，用于日志或调试，可为空。
// param Begin: 执行前置钩子，允许在 Gin 上下文中开启 Trace、补充标签等；返回新的 context 及收尾函数。
// param ReadBody: 读取 HTTP 请求体的函数，留空时默认一次性读取并复位 Body。
// param Decode: 将请求体反序列化为业务实体的函数，必填。
// param Enhance: 在解码后对实体做补充处理或额外解析（如解析状态字段），允许为 nil。
// param Validate: 对实体执行业务校验的钩子，允许为 nil。
// param Prepare: 在调用 Service 前执行的准备操作（如设置默认值），允许为 nil。
// param WithTimeout: 设置后续 Service 调用的上下文和取消函数，允许为 nil。
// param InvokeService: 实际调用 Service 层的函数，必填。
// param AfterService: Service 成功后执行的补充钩子，例如记录 Trace、指标等，允许为 nil。
// param SuccessPayload: 构建成功响应体的函数，允许为 nil。
// param ResponseWriter: 输出响应的函数，需负责错误与成功两种场景，允许为 nil。
type HandlerConfig[T any] struct {
	Name           string
	Begin          func(*gin.Context) (context.Context, func(error))
	ReadBody       func(*gin.Context) ([]byte, error)
	Decode         func(*gin.Context, []byte) (T, error)
	Enhance        func(*gin.Context, T, []byte) error
	Validate       func(*gin.Context, T) error
	Prepare        func(*gin.Context, T) error
	WithTimeout    func(*gin.Context, T) (context.Context, context.CancelFunc, error)
	InvokeService  func(context.Context, T) error
	AfterService   func(*gin.Context, T) error
	SuccessPayload func(*gin.Context, T) (interface{}, error)
	ResponseWriter func(*gin.Context, error, interface{})
}

// Handler 基于配置执行创建控制流程。
type Handler[T any] struct {
	cfg HandlerConfig[T]
}

// NewHandler 创建一个创建控制器处理器。
func NewHandler[T any](cfg HandlerConfig[T]) *Handler[T] {
	return &Handler[T]{cfg: cfg}
}

// Execute 执行通用创建控制流程。
//
// param c: Gin 上下文，要求携带请求与响应对象。
//
// returns: 返回封装的 HandlerResult 及执行过程中产生的错误。
func (h *Handler[T]) Execute(c *gin.Context) (HandlerResult[T], error) {
	var zero HandlerResult[T]
	if h == nil {
		return zero, nil
	}
	if c == nil {
		return zero, nil
	}
	if h.cfg.Decode == nil {
		return zero, nil
	}
	if h.cfg.InvokeService == nil {
		return zero, nil
	}

	var execErr error
	ctx := c.Request.Context()
	if h.cfg.Begin != nil {
		beginCtx, end := h.cfg.Begin(c)
		if beginCtx != nil {
			ctx = beginCtx
			c.Request = c.Request.WithContext(beginCtx)
		}
		if end != nil {
			defer func() {
				end(execErr)
			}()
		}
	}

	readBody := h.cfg.ReadBody
	if readBody == nil {
		readBody = defaultReadBody
	}

	body, err := readBody(c)
	if err != nil {
		execErr = err
		h.writeResponse(c, err, nil)
		return zero, err
	}

	entity, err := h.cfg.Decode(c, body)
	if err != nil {
		execErr = err
		h.writeResponse(c, err, nil)
		return zero, err
	}

	if h.cfg.Enhance != nil {
		if err = h.cfg.Enhance(c, entity, body); err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
	}

	if h.cfg.Validate != nil {
		if err = h.cfg.Validate(c, entity); err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
	}

	if h.cfg.Prepare != nil {
		if err = h.cfg.Prepare(c, entity); err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
	}

	svcCtx := ctx
	var cancel context.CancelFunc
	if h.cfg.WithTimeout != nil {
		svcCtx, cancel, err = h.cfg.WithTimeout(c, entity)
		if err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
		if svcCtx != nil {
			c.Request = c.Request.WithContext(svcCtx)
		} else {
			svcCtx = ctx
		}
	}
	if cancel != nil {
		defer cancel()
	}

	if err = h.cfg.InvokeService(svcCtx, entity); err != nil {
		execErr = err
		h.writeResponse(c, err, nil)
		return zero, err
	}

	if h.cfg.AfterService != nil {
		if err = h.cfg.AfterService(c, entity); err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
	}

	var payload interface{}
	if h.cfg.SuccessPayload != nil {
		payload, err = h.cfg.SuccessPayload(c, entity)
		if err != nil {
			execErr = err
			h.writeResponse(c, err, nil)
			return zero, err
		}
	}

	h.writeResponse(c, nil, payload)
	result := HandlerResult[T]{
		Entity:  entity,
		Payload: payload,
	}
	return result, nil
}

func (h *Handler[T]) writeResponse(c *gin.Context, err error, payload interface{}) {
	if h == nil || h.cfg.ResponseWriter == nil {
		return
	}
	h.cfg.ResponseWriter(c, err, payload)
}

func defaultReadBody(c *gin.Context) ([]byte, error) {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return nil, nil
	}
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(data))
	return data, nil
}
