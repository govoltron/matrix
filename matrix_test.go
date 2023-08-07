package matrix

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var (
	ctx    = context.Background()
	matrix = (*Matrix)(nil)
)

// TestMain
func TestMain(m *testing.M) {
	kvs, err := NewEtcdStore(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	if matrix, err = NewMatrix(ctx, "cu4k6mg398qd", kvs); err != nil {
		panic(err)
	}
	defer matrix.Close(ctx)
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

func TestMatrix(t *testing.T) {
	broker, err := matrix.NewBroker(ctx, "user-core-service")
	if err != nil {
		t.Errorf("NewBroker failed, error is %s", err.Error())
		return
	}
	broker.Watch(&brokerWatcher{})
	broker.Watch(nil)

	defer broker.Close()

	srv, err := matrix.NewService(ctx, "user-core-service")
	if err != nil {
		t.Errorf("NewService failed, error is %s", err.Error())
		return
	}
	defer srv.Close()

	go func() {
		time.Sleep(4 * time.Second)

		addrs := make(map[string]int)
		balance := NewBalancer(broker)
		for i := 0; i < 10000; i++ {
			addr := balance.Next()
			addrs[addr]++
		}

		t.Logf("Balancer addrs: %+v\n", addrs)
	}()

	reporter0 := matrix.NewReporter(ctx, "user-core-service")
	defer reporter0.Close()
	reporter0.Keepalive("114.116.209.130:8099", 100, 2*time.Second)

	reporter1 := matrix.NewReporter(ctx, "user-core-service")
	defer reporter1.Close()
	reporter1.Keepalive("127.0.0.1:8081", 100, 2*time.Second)

	reporter2 := matrix.NewReporter(ctx, "user-core-service")
	defer reporter2.Close()
	reporter2.Keepalive("127.0.0.1:8082", 100, 2*time.Second)

	reporter3 := matrix.NewReporter(ctx, "user-core-service")
	defer reporter3.Close()
	reporter3.Keepalive("127.0.0.1:8083", 1, 2*time.Second)

	matrix.Setenv(ctx, "NAME", matrix.Name())
	srv.Setenv(ctx, "options", `{"host":"open.17paipai.cn","scheme":"http"}`)

	for i := 0; i < 5; i++ {
		value := broker.Getenv(ctx, "options")
		t.Logf("v: %+v\n", value)
		time.Sleep(1 * time.Second)
	}

	srv.Delenv(ctx, "options")
	matrix.Delenv(ctx, "NAME")

	time.Sleep(time.Second * 30)

}

func TestMatrix_ENV(t *testing.T) {
	if err := matrix.Setenv(ctx, "unittest", "1"); err != nil {
		t.Errorf("Setenv failed, error is %s", err.Error())
		return
	}
	if value := matrix.Getenv(ctx, "unittest"); value != "1" {
		t.Errorf("Getenv failed, unexpected result: %s", value)
		return
	}
	if err := matrix.Delenv(ctx, "unittest"); err != nil {
		t.Errorf("Delenv failed, error is %s", err.Error())
		return
	}
	if value := matrix.Getenv(ctx, "unittest"); value != "" {
		t.Errorf("Getenv failed, unexpected result: %s", value)
		return
	}
}
