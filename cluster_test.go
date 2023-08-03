package cluster

import (
	"context"
	"testing"
	"time"
)

var (
	ctx     = context.Background()
	cluster = (*Matrix)(nil)
)

// TestMain
func TestMain(m *testing.M) {
	kvs, err := NewEtcdKVS(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	cluster, err = NewMatrix(ctx, "cu4k6mg398qd", kvs)
	if err != nil {
		panic(err)
	}
	defer cluster.Close(ctx)
	// Run
	m.Run()
}

func TestCluster(t *testing.T) {
	broker, err := cluster.NewBroker(ctx, "user-core-service")
	if err != nil {
		t.Errorf("NewBroker failed, error is %s", err.Error())
		return
	}
	defer broker.Close()

	srv, err := cluster.NewService(ctx, "user-core-service")
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

	reporter0 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter0.Close()
	reporter0.Keepalive("127.0.0.1:8080", 100, 2*time.Second)

	reporter1 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter1.Close()
	reporter1.Keepalive("127.0.0.1:8081", 100, 2*time.Second)

	reporter2 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter2.Close()
	reporter2.Keepalive("127.0.0.1:8082", 100, 2*time.Second)

	reporter3 := cluster.NewReporter(ctx, "user-core-service")
	defer reporter3.Close()
	reporter3.Keepalive("127.0.0.1:8083", 1, 2*time.Second)

	cluster.Setenv(ctx, "NAME", cluster.Name())
	srv.Setenv(ctx, "options", `{"username":"root"}`)

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
