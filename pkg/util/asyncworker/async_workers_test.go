package asyncworker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAsyncWorkers(t *testing.T) {
	rt := require.New(t)

	asw := NewAsyncWorkers("test")

	result, a, b, c, d, e, f := 0, 1, 2, 3, 4, 5, 6

	timeoutSeconds := 10 * time.Second
	fn := func(ctx context.Context, params ...interface{}) error {

		if len(params) != 2 {
			return fmt.Errorf("invalid params")
		}

		time.Sleep(5 * time.Second)
		p1Int := params[0].(int)
		p2Int := params[1].(int)
		result = p1Int + p2Int
		return nil
	}

	work1Name := "work1"
	work1DeliveredAt := time.Now()
	work1 := &Work{
		Fn:          fn,
		Params:      []interface{}{a, b},
		DeliveredAt: work1DeliveredAt,
	}

	err := asw.AddWork(work1Name, work1)
	rt.Nil(err)
	asw.workLock.Lock()
	rt.NotNil(asw.workStatuses[work1Name])
	asw.workLock.Unlock()

	asw.workLock.Lock()
	for asw.workStatuses[work1Name].working {
		asw.workLock.Unlock()
		time.Sleep(1 * time.Second)

		if time.Now().Sub(work1DeliveredAt) > timeoutSeconds {
			rt.Failf("%s timeout", work1Name)
		}

		asw.workLock.Lock()
	}
	asw.workLock.Unlock()

	rt.Equal(result, a+b)

	work2Name := "work2"
	work2DeliveredAt := time.Now()
	work2 := &Work{
		Fn:          fn,
		Params:      []interface{}{c, d},
		DeliveredAt: work2DeliveredAt,
	}

	err = asw.AddWork(work2Name, work2)
	rt.Nil(err)
	asw.workLock.Lock()
	rt.NotNil(asw.workStatuses[work2Name])
	rt.Nil(asw.lastUndeliveredWork[work2Name])
	asw.workLock.Unlock()

	work3DeliveredAt := time.Now()
	work3 := &Work{
		Fn:          fn,
		Params:      []interface{}{e, f},
		DeliveredAt: work3DeliveredAt,
	}

	err = asw.AddWork(work2Name, work3)
	rt.Nil(err)
	asw.workLock.Lock()
	rt.NotNil(asw.lastUndeliveredWork[work2Name])
	asw.workLock.Unlock()

	asw.workLock.Lock()
	for asw.workStatuses[work2Name].working {
		asw.workLock.Unlock()
		time.Sleep(1 * time.Second)

		if time.Now().Sub(work1DeliveredAt) > 3*timeoutSeconds {
			rt.Failf("%s timeout", work2Name)
		}

		asw.workLock.Lock()
	}
	asw.workLock.Unlock()

	rt.Equal(result, e+f)
}
