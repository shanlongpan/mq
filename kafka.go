/**
* @Author: Tristan
* @Date: 2020/6/19 8:12 下午
 */
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const SleepTime = 200 * time.Millisecond

// Handler that handle received kafka messages
type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

type Kafka struct {
	Opts          Options
	Producer      sarama.SyncProducer
	ConsumerGroup sarama.ConsumerGroup
	topics        []string
	handler       Handler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (k *Kafka) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (k *Kafka) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *Kafka) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		k.ConsumeMsg(session, message)
	}

	return nil
}

func (k *Kafka) ConsumeMsg(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	ctx := context.TODO()
	err := handleMessageWithRetry(ctx, k.handler, msg, k.Opts.Retries)
	if err != nil {
		fmt.Println(err)
	}

	session.MarkMessage(msg, "")
}

func handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Panic happened during handle of message: %v")
		}
	}()

	err = handler(ctx, msg)
	if err != nil && retries > 0 {
		time.Sleep(SleepTime)
		return handleMessageWithRetry(ctx, handler, msg, retries-1)
	}
	return err
}

func (k *Kafka) Init(opt ...Option) error {
	opts := newOptions(opt...)

	if len(opts.Addrs) == 0 {
		return errors.New("addr is empty")
	}

	if len(opts.Ver) == 0 {
		log.Printf("%s", "version is empty")
	}

	k.Opts = opts
	return nil
}

func NewKafkaProducer(opt ...Option) (*Kafka, error) {
	k := &Kafka{}
	err := k.Init(opt...)
	if err != nil {
		return k, err
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3 //重试次数最大3，default 是3
	// config.Producer.Partitioner = sarama.NewRandomPartitioner //默认为随机发送到其中一个 partitioner
	config.Producer.Return.Successes = true

	version, err := sarama.ParseKafkaVersion(k.Opts.Ver)
	if err != nil {
		return k, err
	}
	config.Version = version

	producer, err := sarama.NewSyncProducer(k.Opts.Addrs, config)
	if err != nil {
		return k, err
	}
	k.Producer = producer
	return k, nil
}

func NewConsumerGroup(opt ...Option) (*Kafka, error) {
	k := &Kafka{}
	err := k.Init(opt...)
	if err != nil {
		return k, err
	}

	version, err := sarama.ParseKafkaVersion(k.Opts.Ver)
	if err != nil {
		return k, err
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer, err := sarama.NewConsumerGroup(k.Opts.Addrs, k.Opts.GroupId, config)
	if err != nil {
		return k, err
	}
	k.ConsumerGroup = consumer
	go func() {
		err := <-k.ConsumerGroup.Errors()
		if err != nil {
			fmt.Println("sarama error: %s", err.Error())
		}
	}()
	return k, nil
}

func (k *Kafka) Produce(ctx context.Context, topic string, key string, value interface{}) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
	}
	var jsonStr string
	switch value.(type) {
	case string:
		jsonStr = value.(string)
	default:
		jsonValue, err := json.Marshal(value)

		if err != nil {
			return err
		}
		jsonStr = string(jsonValue)
	}

	msg.Value = sarama.StringEncoder(jsonStr)
	partition, offset, err := k.Producer.SendMessage(msg)

	if err != nil {
		return err
	}

	fmt.Println(value, partition, offset)
	return nil
}

func (k *Kafka) Consume(ctx context.Context, topic string, f HandlerMq) error {

	k.handler = func(f HandlerMq) Handler {
		return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return f(ctx, msg.Value)
		}
	}(f)

	ctx, cancel := context.WithCancel(context.Background())
	// k.handler = handler
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := k.ConsumerGroup.Consume(ctx, strings.Split(topic, ","), k); err != nil {
				fmt.Println("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err := k.CloseConsumer(); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (k *Kafka) CloseProducer() error {
	if k.Producer == nil {
		return nil
	}
	return k.Producer.Close()
}

func (k *Kafka) CloseConsumer() error {
	if k.ConsumerGroup == nil {
		return nil
	}
	return k.ConsumerGroup.Close()
}
