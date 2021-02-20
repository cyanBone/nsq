package nsq

import (
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
)

type Producer struct {
	mu         sync.Mutex //参数锁
	nsqAddress string
	producer   *nsq.Producer
}

func (p *Producer) init() error {
	var err error
	if p.producer != nil && p.producer.Ping() == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.producer != nil && p.producer.Ping() == nil {
		return nil
	}
	p.producer, err = nsq.NewProducer(p.nsqAddress, nsq.NewConfig())
	if err != nil {
		return err
	}
	p.producer.SetLogger(nil, 0)
	return nil
}

func (p *Producer) Publish(topic string, message []byte) error {
	err := p.init()
	if err != nil {
		return err
	}

	if message == nil {
		return nil
	}
	return p.producer.Publish(topic, message)
}

func (p *Producer) Stop() {
	if p.producer != nil {
		p.producer.Stop()
	}
}

type handle struct {
	handle func(message []byte) error
}

func (hm *handle) HandleMessage(msg *nsq.Message) error {
	return hm.handle(msg.Body)
}

type Consumer struct {
	nsqAddress string
	consumer   *nsq.Consumer
}

func (nc *Consumer) Consumer(topic string, channel string, thread int, handler func(message []byte) error) error {
	var err error
	cfg := nsq.NewConfig()
	cfg.LookupdPollInterval = time.Second * 15
	nc.consumer, err = nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return err
	}
	nc.consumer.SetLogger(nil, 0)
	nc.consumer.AddConcurrentHandlers(&handle{handle: handler}, thread)
	return nc.consumer.ConnectToNSQD(nc.nsqAddress)
}

func (nc *Consumer) Stop() {
	if nc.consumer != nil {
		nc.consumer.Stop()
	}
}

// 创建nsq消费者
func NewConsumer(connection string) *Consumer {
	return &Consumer{nsqAddress: connection}
}

// 创建nsq生产者
func NewProducer(connection string) *Producer {
	return &Producer{nsqAddress: connection}
}
