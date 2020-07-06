/**
* @Author: Tristan
* @Date: 2020/6/18 9:20 下午
 */
package mq

import (
	"context"
)

type Options struct {
	Addrs   []string // 地址
	Ver     string   // 版本号
	GroupId string   // 分组 id
	Retries int      // 重试次数
}

type Option func(*Options)

func newOptions(options ...Option) Options {
	opts := Options{}

	for _, o := range options {
		o(&opts)
	}

	return opts
}

type HandlerMq func(ctx context.Context, msg []byte) error

type Mq interface {
	Init(...Option) error
	// ConsumeWithHandler(ctx context.Context, topic string, value interface{}) (Handler, error)
	Consume(ctx context.Context, topic string, handler HandlerMq) error
	Produce(ctx context.Context, topic string, key string, value interface{}) error
}

// Addrs is the registry addresses to use
func Addrs(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

func Ver(ver string) Option {
	return func(o *Options) {
		o.Ver = ver
	}
}

func GroupId(groupId string) Option {
	return func(o *Options) {
		o.GroupId = groupId
	}
}

func Retries(retries int) Option {
	return func(o *Options) {
		o.Retries = retries
	}
}

var DefaultConsumeMq Mq
var DefaultProducerMq Mq

func initConsumer(opt ...Option) error {
	var err error
	// DefaultConsumeMq, err = NewConsumerGroup(Addrs("127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"), Ver("2.3.1"), GroupId("consume-1"), Retries(3))
	DefaultConsumeMq, err = NewConsumerGroup(opt...)
	if err != nil {
		return err
	}
	return nil
}

func initProduce(opt ...Option) error {
	var err error
	// DefaultProducerMq, err = NewKafkaProducer(Addrs("127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"), Ver("2.3.1"))
	DefaultProducerMq, err = NewKafkaProducer(opt...)
	if err != nil {
		return err
	}
	return nil
}

func WriteMsg(ctx context.Context, topic string, key string, value interface{}) error {

	return DefaultProducerMq.Produce(ctx, topic, key, value)
}
func Consume(ctx context.Context, topic string, mqHander HandlerMq) error {

	return DefaultConsumeMq.Consume(ctx, topic, mqHander)
}
