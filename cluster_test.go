package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type CustomKeyParser struct{}

// Resolve implements KeyParser.
func (kp *CustomKeyParser) Resolve(name string) (key string) {
	switch {
	case strings.HasSuffix(name, "-service"):
		return fmt.Sprintf("/services/%s", name[0:len(name)-8])
	}
	return fmt.Sprintf("/unknown/%s", name)
}

func TestCluster(t *testing.T) {
	ctx := context.TODO()

	kvs, err := NewEtcdKVS(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("NewEtcdKVS failed, error is %s", err.Error())
		return
	}

	cluster := NewCluster(ctx, kvs, "cu4k6mg398qd", WithMatrixKeyParser(&CustomKeyParser{}))
	defer cluster.Close(context.TODO())

	broker := cluster.NewBroker(ctx, "user-core-service")
	defer broker.Close()

	go func() {
		for i := 0; i < 10; i++ {
			fmt.Printf("endpoints: %+v\n", broker.Endpoints())
			time.Sleep(time.Second)
		}
	}()

	reporter := cluster.NewReporter(ctx, "user-core-service")
	reporter.Keepalive("127.0.0.1:8080", 100, 2*time.Second)
	time.Sleep(time.Second * 4)
	reporter.Keepalive("127.0.0.1:8081", 100, 2*time.Second)
	time.Sleep(time.Second * 10)
	reporter.Cancel()
	defer reporter.Close()

	time.Sleep(time.Second * 10)

	balancer := NewBalancer(broker)
	addr := balancer.Acquire()
	balancer.Release(addr)

}
