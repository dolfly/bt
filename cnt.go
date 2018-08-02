package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Count struct {
	Start   time.Time
	Finish  time.Time
	Success int
	Fail    int
	Data    []float64
	mu      sync.Mutex
}

func NewCount() *Count {
	return &Count{
		Start: time.Now(),
	}
}

func (c *Count) Result() {
	c.Finish = time.Now()
	cost := c.Finish.UnixNano()/1e6 - c.Start.UnixNano()/1e6
	costAvg := fmt.Sprintf("%.3f", float64(cost)/float64(c.Success+c.Fail))
	qps := int64(float64(c.Success+c.Fail) / (float64(cost) / 1e3))
	sort.Float64s(c.Data)
	cost95 := fmt.Sprintf("%.3f", c.Data[len(c.Data)*95/100])
	cost99 := fmt.Sprintf("%.3f", c.Data[len(c.Data)*99/100])

	fmt.Printf("time[%ds] total[%d] fail[%d] QPS[%d] cost_avg[%sms] cost_99[%sms] cost_95[%sms]\n",
		cost/1e3, c.Success+c.Fail, c.Fail, qps, costAvg, cost99, cost95)
}

func (c *Count) AddData(data []float64) {
	c.mu.Lock()
	c.Data = append(c.Data, data...)
	c.mu.Unlock()
}

func (c *Count) Reset() {
	t := &Count{}
	*c = *t
	c.Start = time.Now()
}
