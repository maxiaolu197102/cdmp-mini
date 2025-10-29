package user

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/trace"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	v1 "github.com/maxiaolu1981/cretem/nexuscore/api/apiserver/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/component-base/fields"
	metav1 "github.com/maxiaolu1981/cretem/nexuscore/component-base/meta/v1"
	"github.com/maxiaolu1981/cretem/nexuscore/errors"
)

func (u *UserService) List(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (result *v1.UserList, err error) {
	serviceCtx, serviceSpan := trace.StartSpan(ctx, "user-service", "list_users")
	if serviceCtx != nil {
		ctx = serviceCtx
	}

	spanStatus := "success"
	businessCode := strconv.Itoa(code.ErrSuccess)
	spanDetails := map[string]any{
		"field_selector": opts.FieldSelector,
	}
	outcomeStatus := "success"
	outcomeCode := businessCode
	outcomeMessage := ""
	outcomeHTTP := http.StatusOK

	defer func() {
		if err != nil {
			spanStatus = "error"
			outcomeStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				businessCode = strconv.Itoa(c)
				outcomeCode = businessCode
			} else {
				businessCode = strconv.Itoa(code.ErrUnknown)
				outcomeCode = businessCode
			}
			if msg := errors.GetMessage(err); msg != "" {
				outcomeMessage = msg
			}
			if status := errors.GetHTTPStatus(err); status != 0 {
				outcomeHTTP = status
			} else {
				outcomeHTTP = http.StatusInternalServerError
			}
		}
		if opts.Limit != nil {
			spanDetails["limit"] = *opts.Limit
		}
		if opts.Offset != nil {
			spanDetails["offset"] = *opts.Offset
		}
		if serviceSpan != nil {
			trace.EndSpan(serviceSpan, spanStatus, businessCode, spanDetails)
		}
		trace.RecordOutcome(ctx, outcomeCode, outcomeMessage, outcomeStatus, outcomeHTTP)
	}()

	startTime := time.Now()

	var username string
	// 处理字段选择器
	if opts.FieldSelector != "" {
		selector, err := fields.ParseSelector(opts.FieldSelector)
		if err != nil {
			return nil, err
		}
		username, _ = selector.RequiresExactMatch("name")
	}

	if username == "" {
		return nil, errors.WithCode(code.ErrInvalidParameter, "必须指定用户名进行查询")
	}
	trace.AddRequestTag(ctx, "target_user", username)
	spanDetails["target_user"] = username

	//判断用户是否存在
	ruser, err := u.checkUserExist(ctx, username, true)
	if err != nil {
		log.Warnf("查询用户%s checkUserExist方法返回错误: %v", username, err)
	}
	if ruser != nil && (ruser.Name == RATE_LIMIT_PREVENTION || ruser.Name == BLACKLIST_SENTINEL) {
		return nil, errors.WithCode(code.ErrUserNotFound, "用户不存在,无法查询")
	}

	// 步骤1：查询原始用户列表
	users, err := u.Store.Users().List(ctx, username, opts, u.Options)
	if err != nil {
		log.Errorf("List users from storage failed: %v", err)
		return nil, errors.WithCode(code.ErrDatabase, "query raw users failed: %v", err)
	}

	if len(users.Items) == 0 {
		logger.Debugf("没有发现用户，返回空列表")
		return &v1.UserList{ListMeta: users.ListMeta, Items: []*v1.User{}}, nil
	}

	logger.Debugf("发现 %d 用户列表, 开始并行处理", len(users.Items))

	var (
		wg                sync.WaitGroup
		mu                sync.Mutex
		hasError          int32
		cancelCtx, cancel = context.WithCancel(ctx)
	)
	defer cancel()

	// 预分配结果切片和索引映射 - 修复：使用 uint64 作为键类型
	resultItems := make([]*v1.User, len(users.Items))
	idToIndex := make(map[uint64]int, len(users.Items)) // 改为 uint64
	for i, user := range users.Items {
		idToIndex[user.ID] = i      // 现在类型匹配了
		resultItems[i] = &v1.User{} // 预分配内存，避免nil
	}

	// 工作池：限制并发数
	userChan := make(chan *v1.User, u.Options.ServerRunOptions.MaxQueueSize)
	errChan := make(chan error, 1) // 只需要一个错误信号

	// 启动worker池
	for i := 0; i < u.Options.ServerRunOptions.MaxGoroutines; i++ {
		wg.Add(1)
		go u.processUserWorker(cancelCtx, i, userChan, errChan, &wg, &mu, idToIndex, resultItems, &hasError)
	}

	// 步骤3：发送任务到工作池
	go func() {
		defer close(userChan)
		for _, user := range users.Items { // 移除了未使用的 i 变量
			if atomic.LoadInt32(&hasError) == 1 {
				break
			}

			select {
			case userChan <- user:
				// 任务发送成功
			case <-cancelCtx.Done():
				return
			default:
				// 队列满时等待或跳过（根据业务需求调整）
				select {
				case userChan <- user:
				case <-cancelCtx.Done():
					return
				case <-time.After(u.Options.ServerRunOptions.TimeoutThreshold):
					logger.Warnf("User channel full, skipping user %d", user.ID) // 改为 %d
				}
			}
		}
	}()

	// 步骤4：等待完成并处理结果
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 监听完成信号
	select {
	case <-cancelCtx.Done():
		err := errors.WithCode(code.ErrContextCanceled, "list users canceled: %v", ctx.Err())
		logger.Errorf("Operation canceled: %v", err)
		return nil, err

	case err, ok := <-errChan:
		if ok && err != nil {
			logger.Errorf("Parallel processing failed: %v", err)
			return nil, err
		}
	}

	// 统计处理时间
	processingTime := time.Since(startTime)
	logger.Debugf("Successfully processed %d users in %v", len(resultItems), processingTime)

	result = &v1.UserList{
		ListMeta: users.ListMeta,
		Items:    resultItems,
	}
	spanDetails["returned_count"] = len(resultItems)
	return result, nil
}

// processUserWorker 处理单个用户的worker
func (u *UserService) processUserWorker(
	ctx context.Context,
	workerID int,
	userChan <-chan *v1.User,
	errChan chan<- error,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	idToIndex map[uint64]int, // 改为 uint64
	resultItems []*v1.User,
	hasError *int32,
) {
	defer wg.Done()
	logger := log.L(ctx).WithValues("worker", workerID)

	for user := range userChan {
		// 快速检查错误状态
		if atomic.LoadInt32(hasError) == 1 {
			return
		}

		// 检查上下文取消
		select {
		case <-ctx.Done():
			return
		default:
		}

		itemCtx, itemSpan := trace.StartSpan(ctx, "user-service", "process_user")
		spanStatus := "success"
		spanCode := strconv.Itoa(code.ErrSuccess)
		spanDetails := map[string]any{
			"user_id":  user.ID,
			"username": user.Name,
		}

		// 处理单个用户（带超时控制）
		processedUser, err := u.processSingleUserWithTimeout(itemCtx, user)
		if err != nil {
			if atomic.CompareAndSwapInt32(hasError, 0, 1) {
				select {
				case errChan <- err:
				default: // 避免阻塞
				}
			}
			spanStatus = "error"
			if c := errors.GetCode(err); c != 0 {
				spanCode = strconv.Itoa(c)
			}
			if itemSpan != nil {
				trace.EndSpan(itemSpan, spanStatus, spanCode, spanDetails)
			}
			return
		}

		// 安全写入结果
		mu.Lock()
		if idx, exists := idToIndex[user.ID]; exists { // 现在类型匹配了
			resultItems[idx] = processedUser
		} else {
			logger.Warnf("User ID %d not found in index map", user.ID)
		}
		mu.Unlock()
		if itemSpan != nil {
			spanDetails["policy_count"] = processedUser.TotalPolicy
			trace.EndSpan(itemSpan, spanStatus, spanCode, spanDetails)
		}
	}
}

// processSingleUserWithTimeout 带超时控制的单个用户处理
func (u *UserService) processSingleUserWithTimeout(ctx context.Context, user *v1.User) (*v1.User, error) {

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 查询用户策略 - 取消注释并根据实际情况实现
	policies, err := u.Store.Polices().List(ctx, user.Name, metav1.ListOptions{})
	if err != nil {
		return nil, errors.WithCode(code.ErrDatabase, "query policies for user %s failed: %v", user.Name, err)
	}
	processedUser := *user
	processedUser.TotalPolicy = policies.TotalCount
	return &processedUser, nil

}

func (u *UserService) ListWithBadPerformance(ctx context.Context, opts metav1.ListOptions, opt *options.Options) (*v1.UserList, error) {
	return nil, nil
}
