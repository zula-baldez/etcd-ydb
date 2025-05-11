package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	"github.com/ydb-platform/etcd-ydb/pkg/etcd"
	"github.com/ydb-platform/etcd-ydb/pkg/report"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

var watchPutCmd = &cobra.Command{
	Use:  "watch-put",
	RunE: watchPutFunc,
}

var (
	watchPutTotal        uint64
	watchPutRateLimit    uint64
	watchPutKeySize      uint64
	watchPutValSize      uint64
	watchPutKeySpaceSize uint64
)

func init() {
	RootCmd.AddCommand(watchPutCmd)
	watchPutCmd.Flags().Uint64Var(&watchPutTotal, "total", 10000, "Total number of requests")
	watchPutCmd.Flags().Uint64Var(&watchPutRateLimit, "rate-limit", math.MaxUint64, "Maximum requests per second")
	watchPutCmd.Flags().Uint64Var(&watchPutKeySize, "key-size", 8, "Key size of request")
	watchPutCmd.Flags().Uint64Var(&watchPutValSize, "val-size", 8, "Value size of request")
	watchPutCmd.Flags().Uint64Var(&watchPutKeySpaceSize, "key-space-size", 1, "Maximum possible keys")
}

func watchPutFunc(_ *cobra.Command, _ []string) error {
	clients, err := newClients()
	if err != nil {
		return err
	}
	limit := rate.NewLimiter(rate.Limit(watchPutRateLimit), 1)

	bar := pb.New64(int64(watchPutTotal))
	bar.Start()

	ops := make(chan etcd.Request, totalClients)
	rep := report.NewReport(totalClients)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	var revisionTimestamps sync.Map
	for i := range clients {
		client := clients[i]
		for j := 0; j < int(len(clients)); j++ {
			go func(client *etcd.Client, streamID int) {
				defer cancel()

				watch, err := client.Watch(ctx)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to create watch stream: %v\n", err)
					return
				}

				err = watch.Send(
					&etcdserverpb.WatchRequest{
						RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
							CreateRequest: &etcdserverpb.WatchCreateRequest{
								Key:      []byte(""),
								RangeEnd: []byte("\x00"),
							},
						},
					},
				)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to send watch request: %v\n", err)
					return
				}

				for {
					resp, err := watch.Recv()
					if err != nil {
						fmt.Fprintf(os.Stderr, "Watch stream %d error: %v\n", streamID, err)
						break
					}

					for _, event := range resp.Events {
						if event.Kv != nil {
							rev := event.Kv.ModRevision
							if putTimeRaw, ok := revisionTimestamps.Load(rev); ok {
								putTime := putTimeRaw.(time.Time)
								latency := time.Since(putTime)

								rep.Results() <- report.Result{
									TotalTime: latency,
									Err:       nil,
								}
							} else {

								revisionTimestamps.Store(rev, time.Now())
							}
						}
					}
				}
			}(client, j)
		}
	}
	for i := range clients {
		wg.Add(1)
		go func(client *etcd.Client) {
			defer wg.Done()
			for op := range ops {
				limit.Wait(context.Background())

				resp, err := etcd.Do(context.Background(), client, op)
				if err == nil {
					if putResp, ok := resp.(*etcd.PutResponse); ok {
						var rev = putResp.Revision
						if putTimeRaw, ok := revisionTimestamps.Load(rev); ok {
							putTime := putTimeRaw.(time.Time)
							latency := time.Since(putTime)

							rep.Results() <- report.Result{
								TotalTime: latency,
								Err:       nil,
							}
						} else {

							revisionTimestamps.Store(rev, time.Now())
						}
					}

				}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		key, value := []byte(strings.Repeat("-", int(watchPutKeySize))), strings.Repeat("-", int(watchPutValSize))
		for range watchPutTotal {
			j := 0
			for n := rand.Uint64() % watchPutKeySpaceSize; n > 0; n /= 10 {
				key[j] = byte('0' + n%10)
				j++
			}
			slices.Reverse(key[:j])
			op := &etcd.PutRequest{Key: string(key), Value: value}
			ops <- op
		}
		close(ops)
	}()

	rc := rep.Run()
	wg.Wait()
	cancel()
	close(rep.Results())
	bar.Finish()
	stats := <-rc
	fmt.Fprintf(os.Stderr, "%#v\n", stats)
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
