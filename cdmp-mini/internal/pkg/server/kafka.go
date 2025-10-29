package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
	"github.com/segmentio/kafka-go"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/apiserver/options"
)

// CheckKafkaConnection 检查 Kafka 连接
func CheckKafkaConnection(opt *options.Options) error {
	var lastErr error

	for i := 0; i < opt.KafkaOptions.MaxRetries; i++ {

		// 方法1: 尝试连接 Broker
		if err := checkKafkaBrokers(opt.KafkaOptions.Brokers, opt.KafkaOptions.BatchTimeout); err != nil {
			lastErr = err
			log.Debugf("Kafka 连接失败: %v", err)
			time.Sleep(2 * time.Second) // 等待2秒后重试
			continue
		}

		// 方法2: 尝试创建临时主题来验证集群可用性
		if err := checkKafkaCluster(opt); err != nil {
			lastErr = err
			log.Debugf("Kafka 集群检查失败: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Debug("Kafka 连接成功")
		return nil
	}

	return fmt.Errorf("kafka 连接失败，已达到最大重试次数: %v", lastErr)
}

// checkKafkaBrokers 检查 Kafka Broker 连接
func checkKafkaBrokers(brokers []string, timeout time.Duration) error {
	_, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for _, broker := range brokers {
		conn, err := net.DialTimeout("tcp", broker, timeout)
		if err != nil {
			return fmt.Errorf("无法连接到 Broker %s: %v", broker, err)
		}
		conn.Close()
		log.Debugf("Broker %s 连接成功", broker)
	}
	return nil
}

// checkKafkaCluster 检查 Kafka 集群可用性
func checkKafkaCluster(opt *options.Options) error {
	ctx, cancel := context.WithTimeout(context.Background(), opt.KafkaOptions.BatchTimeout)
	defer cancel()

	// 尝试创建临时消费者来验证集群
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: opt.KafkaOptions.Brokers,
		Topic:   opt.KafkaOptions.Topic,
		GroupID: "health-check",
		MaxWait: 1 * time.Second,
	})
	defer reader.Close()

	// 尝试获取集群元数据
	if _, err := reader.ReadMessage(ctx); err != nil {
		// 如果是超时或连接错误，可能是 Kafka 还没完全启动
		if isKafkaStartingError(err) {
			return err
		}
		// 其他错误（如主题不存在）可以忽略，说明 Kafka 是运行的
		log.Debugf("Kafka 运行中但主题可能不存在: %v", err)
	}

	return nil
}

// isKafkaStartingError 判断是否是 Kafka 启动中的错误
func isKafkaStartingError(err error) bool {
	// 这些错误通常表示 Kafka 还在启动中
	switch e := err.(type) {
	case *net.OpError:
		return true
	case kafka.Error:
		return e.Temporary()
	default:
		return false
	}
}

// TestKafkaConnect 初始化 Kafka 连接，如果失败则退出程序
func TestKafkaConnect(opt *options.Options) error {
	log.Debug("开始检查 Kafka 服务状态...")

	if err := CheckKafkaConnection(opt); err != nil {
		log.Fatalf("❌ Kafka 服务不可用，程序退出: %v", err)
		return err
	}

	log.Debug("✅ Kafka 服务可用，继续启动程序...")
	return nil
}
