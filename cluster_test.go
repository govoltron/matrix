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

	cluster := NewCluster(ctx, "cu4k6mg398qd", kvs, WithMatrixKeyParser(&CustomKeyParser{}))
	defer cluster.Close(context.TODO())

	broker := cluster.NewBroker(ctx, "user-core-service")
	defer broker.Close()

	go func() {
		time.Sleep(4 * time.Second)

		balance := NewBalancer(broker)
		for i := 0; i < 20; i++ {
			addr := balance.Next()
			fmt.Printf("addr: %s\n", addr)
			if addr == "127.0.0.1:8081" {
				balance.Unhealth(addr)
			}
			time.Sleep(time.Second)
		}
	}()

	reporter1 := cluster.NewReporter(ctx, "user-core-service")
	reporter1.Keepalive("127.0.0.1:8080", 100, 2*time.Second)
	reporter2 := cluster.NewReporter(ctx, "user-core-service")
	reporter2.Keepalive("127.0.0.1:8081", 300, 2*time.Second)
	reporter3 := cluster.NewReporter(ctx, "user-core-service")
	reporter3.Keepalive("127.0.0.1:8082", 200, 2*time.Second)
	reporter4 := cluster.NewReporter(ctx, "user-core-service")
	reporter4.Keepalive("127.0.0.1:8083", 100, 2*time.Second)

	defer func() {
		reporter1.Close()
		reporter2.Close()
		reporter3.Close()
		reporter4.Close()
	}()

	time.Sleep(time.Second * 30)

}
