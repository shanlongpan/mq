/**
* @Author: Tristan
* @Date: 2020/6/18 9:20 下午
 */
package mq

import (
	"context"
	"fmt"
	"sync"
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

var onceConsume sync.Once
var onceProduce sync.Once

func initConsumer() error {
	var err error
	DefaultConsumeMq, err = NewConsumerGroup(Addrs("127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"), Ver("2.3.1"), GroupId("consume-1"), Retries(3))
	if err != nil {
		return err
	}
	return nil
}

func initProduce() error {
	var err error
	DefaultProducerMq, err = NewKafkaProducer(Addrs("127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"), Ver("2.3.1"))
	if err != nil {
		return err
	}
	return nil
}

func WriteMsg(ctx context.Context, topic string, key string, value interface{}) error {
	onceProduce.Do(func() {
		err := initProduce()
		if err != nil {
			fmt.Println(err)
		}
	})
	return DefaultProducerMq.Produce(ctx, topic, key, value)
}
func Consume(ctx context.Context, topic string, mqHander HandlerMq) error {
	onceConsume.Do(func() {
		err := initConsumer()
		if err != nil {
			fmt.Println(err)
		}
	})
	return DefaultConsumeMq.Consume(ctx, topic, mqHander)
}