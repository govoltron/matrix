package matrix

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/govoltron/matrix/balance"
)

var (
	ctx     = context.Background()
	cluster = (*Cluster)(nil)
)

// TestMain
func TestMain(m *testing.M) {
	kvs, err := NewEtcdStore(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	if cluster, err = NewCluster(ctx, "cu4k6mg398qd", kvs); err != nil {
		panic(err)
	}
	defer cluster.Close(ctx)
	// Run
	m.Run()
}

type brokerWatcher struct {
}

// OnSetenv implements BrokerWatcher.
func (*brokerWatcher) OnSetenv(key string, value string) {
	fmt.Printf("OnSetenv: %s %s\n", key, value)
}

// OnDelenv implements BrokerWatcher.
func (*brokerWatcher) OnDelenv(key string) {

}

// OnUpdateEndpoint implements BrokerWatcher.
func (*brokerWatcher) OnUpdateEndpoint(endpoint Endpoint) {
	fmt.Printf("OnUpdateEndpoint: %+v\n", endpoint)
}

// OnDeleteEndpoint implements BrokerWatcher.
func (*brokerWatcher) OnDeleteEndpoint(id string) {

}

func TestCluster(t *testing.T) {
	broker, err := cluster.NewBroker(ctx, "user-core-service")
	if err != nil {
		t.Errorf("NewBroker failed, error is %s", err.Error())
		return
	}
	broker.Watch(&brokerWatcher{})
	broker.Watch(nil)

	defer broker.Close()

	srv, err := cluster.NewService(ctx, "user-core-service")
	if err != nil {
		t.Errorf("NewService failed, error is %s", err.Error())
		return
	}
	defer srv.Close()

	go func() {
		time.Sleep(5 * time.Second)
		balancer := balance.NewRoundRobinBalancer()
		for _, endpoint := range broker.Endpoints() {
			balancer.Add(endpoint.Addr)
		}
		stat := make(map[string]int)
		for i := 0; i < 10000; i++ {
			stat[balancer.Next()]++
			// if i == 9000 {
			// 	balancer.Remove("127.0.0.1:8083")
			// }
		}

		fmt.Printf("stat: %+v\n", stat)
	}()

	reporter0 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter0.Close(ctx)
	reporter0.Keepalive("114.116.209.130:8099", 100, 2)

	reporter1 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter1.Close(ctx)
	reporter1.Keepalive("127.0.0.1:8081", 100, 2)

	reporter2 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter2.Close(ctx)
	reporter2.Keepalive("127.0.0.1:8082", 100, 2)

	reporter3 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter3.Close(ctx)
	reporter3.Keepalive("127.0.0.1:8083", 1, 2)

	cluster.Setenv(ctx, "NAME", cluster.Name())
	srv.Setenv(ctx, "options", `{"host":"open.17paipai.cn","scheme":"http"}`)

	for i := 0; i < 5; i++ {
		value := broker.Getenv(ctx, "options")
		t.Logf("v: %+v\n", value)
		time.Sleep(1 * time.Second)
	}

	srv.Delenv(ctx, "options")
	cluster.Delenv(ctx, "NAME")

	time.Sleep(time.Second * 30)

}

func TestCluster_ENV(t *testing.T) {
	if err := cluster.Setenv(ctx, "unittest", "1"); err != nil {
		t.Errorf("Setenv failed, error is %s", err.Error())
		return
	}
	if value := cluster.Getenv(ctx, "unittest"); value != "1" {
		t.Errorf("Getenv failed, unexpected result: %s", value)
		return
	}
	if err := cluster.Delenv(ctx, "unittest"); err != nil {
		t.Errorf("Delenv failed, error is %s", err.Error())
		return
	}
	if value := cluster.Getenv(ctx, "unittest"); value != "" {
		t.Errorf("Getenv failed, unexpected result: %s", value)
		return
	}
}
