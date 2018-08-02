package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	aerospike "github.com/aerospike/aerospike-client-go"
)

type BenchmarkAerospike struct {
	RP          *aerospike.Client
	countRead   *Count
	countWrite  *Count
	writePolicy *aerospike.WritePolicy
	readPolicy  *aerospike.BasePolicy
}

func NewBenchmarkAerospike() (*BenchmarkAerospike, error) {
	bm := &BenchmarkAerospike{
		countRead:  NewCount(),
		countWrite: NewCount(),
	}
	if err := bm.init(); err != nil {
		return nil, err
	}
	bm.writePolicy = aerospike.NewWritePolicy(0, 0)
	bm.writePolicy.Timeout = 1 * time.Second
	bm.writePolicy.CommitLevel = aerospike.COMMIT_MASTER
	bm.readPolicy = aerospike.NewPolicy()
	bm.readPolicy.MaxRetries = 10

	return bm, nil
}

func (bm *BenchmarkAerospike) init() error {
	var (
		err   error
		hosts []*aerospike.Host
	)
	for _, v := range Cfg.Aerospike.Host {
		arr := strings.Split(v, ":")
		h := arr[0]
		p, _ := strconv.Atoi(arr[1])
		hosts = append(hosts, aerospike.NewHost(h, p))
	}
	cp := aerospike.NewClientPolicy()
	cp.ConnectionQueueSize = Cfg.Common.Thread
	bm.RP, err = aerospike.NewClientWithPolicyAndHost(cp, hosts...)
	if err != nil {
		return err
	}
	return nil
}

func (bm *BenchmarkAerospike) GenData() {
	var (
		wg sync.WaitGroup
		n  = Cfg.Common.KeyNum / Cfg.Common.Thread
	)

	for i := 0; i < Cfg.Common.Thread; i++ {
		wg.Add(1)

		go func(idx int) {
			for j := idx * n; j < (idx+1)*n; j++ {
				key := fmt.Sprintf(KeyPrefix, j)
				value := fmt.Sprintf(Cfg.Common.ValFmt, time.Now().UnixNano())

				k, err := aerospike.NewKey(Cfg.Aerospike.Namespace, Cfg.Aerospike.Set, key)
				if err != nil {
					fmt.Println(err)
					return
				}
				bin := &aerospike.Bin{Name: Cfg.Aerospike.Bin, Value: aerospike.NewStringValue(value)}
				if err = bm.RP.PutBins(bm.writePolicy, k, bin); err != nil {
					fmt.Println(err)
				}
			}

			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (bm *BenchmarkAerospike) Start() {
	bm.countRead.Reset()
	bm.countWrite.Reset()
	bm.StartRead()

	bm.countRead.Reset()
	bm.countWrite.Reset()
	bm.StartWrite()

	bm.countRead.Reset()
	bm.countWrite.Reset()
	bm.StartReadWrite()
}

func (bm *BenchmarkAerospike) StartRead() {
	var wg sync.WaitGroup
	for i := 0; i < Cfg.Common.Thread; i++ {
		wg.Add(1)
		go func(idx int) {
			bm.startRead(idx)
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Println("===== benchmark aerospike read =====")
	bm.countRead.Result()
}

func (bm *BenchmarkAerospike) startRead(idx int) {
	var (
		n             = Cfg.Common.KeyNum / Cfg.Common.Thread
		key           string
		fail, success int
		cost          []float64
	)
	for i := idx * n; i < (idx+1)*n; i++ {
		ts := time.Now().UnixNano()
		key = fmt.Sprintf(KeyPrefix, i)

		k, err := aerospike.NewKey(Cfg.Aerospike.Namespace, Cfg.Aerospike.Set, key)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = bm.RP.Get(bm.readPolicy, k, Cfg.Aerospike.Bin)
		if err != nil {
			fmt.Println(err)
			fail++
		} else {
			success++
		}
		te := time.Now().UnixNano()
		cost = append(cost, float64(te-ts)/1e6)
	}

	bm.countRead.mu.Lock()
	bm.countRead.Fail += fail
	bm.countRead.Success += success
	bm.countRead.Data = append(bm.countRead.Data, cost...)
	bm.countRead.mu.Unlock()
}

func (bm *BenchmarkAerospike) StartWrite() {
	var wg sync.WaitGroup
	for i := 0; i < Cfg.Common.Thread; i++ {
		wg.Add(1)
		go func(idx int) {
			bm.startWrite(idx)
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Println("===== benchmark aerospike write =====")
	bm.countWrite.Result()
}

func (bm *BenchmarkAerospike) startWrite(idx int) {
	var (
		n             = Cfg.Common.KeyNum / Cfg.Common.Thread
		key, value    string
		fail, success int
		cost          []float64
	)
	for i := idx * n; i < (idx+1)*n; i++ {
		ts := time.Now().UnixNano()
		key = fmt.Sprintf(KeyPrefix, i)
		value = fmt.Sprintf(Cfg.Common.ValFmt, time.Now().UnixNano())

		k, err := aerospike.NewKey(Cfg.Aerospike.Namespace, Cfg.Aerospike.Set, key)
		if err != nil {
			fmt.Println(err)
			return
		}
		bin := &aerospike.Bin{Name: Cfg.Aerospike.Bin, Value: aerospike.NewStringValue(value)}
		err = bm.RP.PutBins(bm.writePolicy, k, bin)
		if err != nil {
			fmt.Println(err)
			fail++
		} else {
			success++
		}
		te := time.Now().UnixNano()
		cost = append(cost, float64(te-ts)/1e6)
	}

	bm.countWrite.mu.Lock()
	bm.countWrite.Fail += fail
	bm.countWrite.Success += success
	bm.countWrite.Data = append(bm.countWrite.Data, cost...)
	bm.countWrite.mu.Unlock()
}

func (bm *BenchmarkAerospike) StartReadWrite() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		var wgr sync.WaitGroup
		for i := 0; i < Cfg.Common.Thread; i++ {
			wgr.Add(1)
			go func(idx int) {
				bm.startRead(idx)
				wgr.Done()
			}(i)
		}
		wgr.Wait()

		fmt.Println("===== benchmark aerospike 50%-read =====")
		bm.countRead.Result()

		wg.Done()
	}()

	wg.Add(1)
	go func() {
		var wgw sync.WaitGroup
		for i := 0; i < Cfg.Common.Thread; i++ {
			wgw.Add(1)
			go func(idx int) {
				bm.startWrite(idx)
				wgw.Done()
			}(i)
		}
		wgw.Wait()

		fmt.Println("===== benchmark aerospike 50%-write =====")
		bm.countWrite.Result()

		wg.Done()
	}()

	wg.Wait()
}
