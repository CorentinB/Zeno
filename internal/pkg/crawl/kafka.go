package crawl

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

type kafkaMessage struct {
	URL       string `json:"u"`
	HopsCount uint8  `json:"hop"`
	ParentURL string `json:"parent_url"`
}

func (crawl *Crawl) kafkaProducer() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(crawl.KafkaBrokers[:], ",")})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logWarning.WithFields(logrus.Fields{
						"error":     ev.TopicPartition.Error,
						"partition": ev.TopicPartition,
						"msg":       ev.String(),
					}).Warning("Kafka message delivery failed")
				} else {
					logInfo.WithFields(logrus.Fields{
						"partition": ev.TopicPartition,
						"msg":       ev.String(),
					}).Debug("Kafka message delivered")
				}
			}
		}
	}()

	for item := range crawl.KafkaProducerChannel {
		if crawl.Finished.Get() {
			break
		}

		var newKafkaMessage = new(kafkaMessage)

		newKafkaMessage.URL = item.URL.String()
		newKafkaMessage.HopsCount = item.Hop
		if item.ParentItem != nil {
			newKafkaMessage.ParentURL = item.ParentItem.URL.String()
		}

		newKafkaMessageBytes, err := json.Marshal(newKafkaMessage)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
			}).Warning("Unable to marshal message before sending to Kafka")
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &crawl.KafkaOutlinksTopic, Partition: kafka.PartitionAny},
			Value:          newKafkaMessageBytes,
		}, nil)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
			}).Warning("Failed to produce message to Kafka, pushing the seed to the local queue instead")
			crawl.Frontier.PushChan <- item
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}

func (crawl *Crawl) kafkaConsumer() {
	kafkaClient, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(crawl.KafkaBrokers[:], ","),
		"group.id":                 crawl.KafkaConsumerGroup,
		"session.timeout.ms":       60000,
		"max.poll.interval.ms":     60000,
		"go.events.channel.enable": true,
	})
	if err != nil {
		panic(err)
	}

	kafkaClient.SubscribeTopics([]string{crawl.KafkaFeedTopic}, nil)

	logrus.WithFields(logrus.Fields{
		"brokers": crawl.KafkaBrokers,
		"group":   crawl.KafkaConsumerGroup,
		"topic":   crawl.KafkaFeedTopic,
	}).Info("Kafka consumer started, it may take some time to actually start pulling messages..")

	for {
		if crawl.Finished.Get() {
			kafkaClient.Close()
			break
		}

		if crawl.Paused.Get() {
			time.Sleep(time.Second)
		}

		if crawl.ActiveWorkers.Value() >= int64(crawl.Workers-(crawl.Workers/10)) {
			time.Sleep(time.Millisecond * 100)
			continue
		}

		select {
		case ev := <-kafkaClient.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				logWarning.WithFields(logrus.Fields{
					"event": e,
				}).Warning("Kafka consumer event")
				kafkaClient.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				logWarning.WithFields(logrus.Fields{
					"event": e,
				}).Warning("Kafka consumer event")
				kafkaClient.Unassign()
			case *kafka.Message:
				var newKafkaMessage = new(kafkaMessage)
				var newItem = new(frontier.Item)
				var newParentItemHops uint8

				logInfo.WithFields(logrus.Fields{
					"value": string(e.Value),
					"key":   string(e.Key),
				}).Debug("New message received from Kafka")

				err = json.Unmarshal(e.Value, &newKafkaMessage)
				if err != nil {
					logWarning.WithFields(logrus.Fields{
						"topic":     crawl.KafkaFeedTopic,
						"key":       e.Key,
						"value":     e.Value,
						"partition": e.TopicPartition,
						"error":     err,
					}).Warning("Unable to unmarshal message from Kafka")
					continue
				}

				// Parse new URL
				newURL, err := url.Parse(newKafkaMessage.URL)
				if err != nil {
					logWarning.WithFields(logrus.Fields{
						"kafka_msg_url": newKafkaMessage.URL,
						"error":         err,
					}).Warning("Unable to parse URL from Kafka message")
					continue
				}

				// If the message specify a parent URL, let's construct a parent item
				if len(newKafkaMessage.ParentURL) > 0 {
					newParentURL, err := url.Parse(newKafkaMessage.ParentURL)
					if err != nil {
						logWarning.WithFields(logrus.Fields{
							"kafka_msg_url": newKafkaMessage.URL,
							"error":         err,
						}).Warning("Unable to parse parent URL from Kafka message")
					} else {
						if newKafkaMessage.HopsCount > 0 {
							newParentItemHops = newKafkaMessage.HopsCount - 1
						}
						newParentItem := frontier.NewItem(newParentURL, nil, "seed", newParentItemHops)
						newItem = frontier.NewItem(newURL, newParentItem, "seed", newKafkaMessage.HopsCount)
					}
				} else {
					newItem = frontier.NewItem(newURL, nil, "seed", newKafkaMessage.HopsCount)
				}

				crawl.Frontier.PushChan <- newItem
			case kafka.PartitionEOF:
				logWarning.WithFields(logrus.Fields{
					"event": e,
				}).Warning("Kafka consumer event")
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				logWarning.WithFields(logrus.Fields{
					"event": e,
				}).Warning("Kafka consumer error")
			}
		}
	}

	kafkaClient.Close()
}
