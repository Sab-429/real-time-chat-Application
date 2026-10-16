package events

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type TopicSpec struct {
	Name       string
	Partitions int
}

// EnsureTopics creates any missing topics (idempotent). With replication >= 3 it
// also sets min.insync.replicas=2 so acks=all actually means something.
func EnsureTopics(ctx context.Context, brokers []string, replication int, specs []TopicSpec) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()

	cfgs := make([]kafka.TopicConfig, 0, len(specs))
	for _, s := range specs {
		tc := kafka.TopicConfig{Topic: s.Name, NumPartitions: s.Partitions, ReplicationFactor: replication}
		if replication >= 3 {
			tc.ConfigEntries = []kafka.ConfigEntry{{ConfigName: "min.insync.replicas", ConfigValue: "2"}}
		}
		cfgs = append(cfgs, tc)
	}
	if err := cc.CreateTopics(cfgs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	return nil
}
