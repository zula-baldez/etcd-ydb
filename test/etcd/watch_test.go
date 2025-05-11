package etcd_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func fillWatchRequest(revision *int64, request *pb.WatchRequest) *pb.WatchRequest {
	if request.GetCreateRequest() != nil && revision != nil {
		request.GetCreateRequest().StartRevision += *revision
	}
	return request
}

func fillWatchResponse(revision *int64, response *pb.WatchResponse) *pb.WatchResponse {
	if response.Header != nil && revision != nil {
		response.Header.Revision += *revision
	}
	return response
}

// TestV3Watch tests Watch APIs from current revision.
func TestV3WatchBase(t *testing.T) {
	tests := []struct {
		name string

		putKeys      []string
		watchRequest *pb.WatchRequest

		wresps []*pb.WatchResponse
	}{
		{
			"watch the key, matching",
			[]string{"foo"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("foo"),
				},
			}},

			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 2},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 2, Version: 1},
						},
					},
				},
			},
		},
		{
			"watch the key, non-matching",
			[]string{"foo"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("helloworld"),
				},
			}},

			[]*pb.WatchResponse{},
		},
		{
			"watch the prefix, matching",
			[]string{"fooLong"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key:      []byte("foo"),
					RangeEnd: []byte("fop"),
				},
			}},

			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 4},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("fooLong"), Value: []byte("bar"), CreateRevision: 4, ModRevision: 4, Version: 1},
						},
					},
				},
			},
		},

		{
			"watch the prefix, non-matching",
			[]string{"foo"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key:      []byte("helloworld"),
					RangeEnd: []byte("helloworle"),
				},
			}},

			[]*pb.WatchResponse{},
		},
		{
			"watch full range, matching",
			[]string{"fooLong"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key:      []byte(""),
					RangeEnd: []byte("\x00"),
				},
			}},

			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 6},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("fooLong"), Value: []byte("bar"), CreateRevision: 4, ModRevision: 6, Version: 2},
						},
					},
				},
			},
		},
		{
			"multiple puts, one watcher with matching key",
			[]string{"foo", "foo", "foo"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("foo"),
				},
			}},
			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 7},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 7, Version: 4},
						},
					},
				},
				{
					Header:  &pb.ResponseHeader{Revision: 8},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 8, Version: 5},
						},
					},
				},
				{
					Header:  &pb.ResponseHeader{Revision: 9},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 9, Version: 6},
						},
					},
				},
			},
		},
		{
			"multiple puts, one watcher with matching prefix",
			[]string{"foo", "foo", "foo"},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key:      []byte("foo"),
					RangeEnd: []byte("fop"),
				},
			}},

			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 10},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 10, Version: 7},
						},
					},
				},
				{
					Header:  &pb.ResponseHeader{Revision: 11},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 11, Version: 8},
						},
					},
				},
				{
					Header:  &pb.ResponseHeader{Revision: 12},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 12, Version: 9},
						},
					},
				},
			},
		},
		{
			"historical revision test",
			[]string{},
			&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key:           []byte(""),
					RangeEnd:      []byte("\x00"),
					StartRevision: 1,
				},
			}},

			[]*pb.WatchResponse{
				{
					Header:  &pb.ResponseHeader{Revision: 12},
					Created: false,
					Events: []*mvccpb.Event{
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 2, Version: 1},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 3, Version: 2},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("fooLong"), Value: []byte("bar"), CreateRevision: 4, ModRevision: 4, Version: 1},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 5, Version: 3},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("fooLong"), Value: []byte("bar"), CreateRevision: 4, ModRevision: 6, Version: 2},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 7, Version: 4},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 8, Version: 5},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 9, Version: 6},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 10, Version: 7},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 11, Version: 8},
						},
						{
							Type: mvccpb.PUT,
							Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 12, Version: 9},
						},
					},
				},
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			wStream, err := client.Watch(ctx)
			if err != nil {
				t.Fatalf("#%d: client.Watch error: %v", i, err)
			}
			err = wStream.Send(tt.watchRequest)
			if err != nil {
				t.Fatalf("#%d: wStream.Send error: %v", i, err)
			}

			cresp, err := wStream.Recv()
			if err != nil {
				t.Fatalf("#%d: wStream.Recv error: %v", i, err)
			}

			if !cresp.Created {
				t.Fatalf("#%d: did not create watchid, got %+v", i, cresp)
			}
			if cresp.Canceled {
				t.Fatalf("#%d: canceled watcher on create %+v", i, cresp)
			}

			createdWatchID := cresp.WatchId
			// if cresp.Header == nil || cresp.Header.Revision != 1 {
			// 	t.Fatalf("#%d: header revision got +%v, wanted revison 1", i, cresp)
			// }

			// asynchronously create keys
			ch := make(chan struct{}, 1)
			go func() {
				for _, k := range tt.putKeys {
					req := &pb.PutRequest{Key: []byte(k), Value: []byte("bar")}
					_, err := client.Put(context.TODO(), req)
					if err != nil {
						t.Errorf("#%d: couldn't put key (%v)", i, err)
					}
					//revision = &res.Header.Revision
				}
				ch <- struct{}{}
			}()

			// check stream results
			for j, wresp := range tt.wresps {
				resp, err := wStream.Recv()
				if err != nil {
					t.Errorf("#%d.%d: wStream.Recv error: %v", i, j, err)
				}

				if resp.Header == nil {
					t.Fatalf("#%d.%d: unexpected nil resp.Header", i, j)
				}
				if resp.Header.Revision != wresp.Header.Revision {
					t.Errorf("#%d.%d: resp.Header.Revision got = %d, want = %d", i, j, resp.Header.Revision, wresp.Header.Revision)
				}

				if wresp.Created != resp.Created {
					t.Errorf("#%d.%d: resp.Created got = %v, want = %v", i, j, resp.Created, wresp.Created)
				}
				if resp.WatchId != createdWatchID {
					t.Errorf("#%d.%d: resp.WatchId got = %d, want = %d", i, j, resp.WatchId, createdWatchID)
				}

				if !reflect.DeepEqual(resp.Events, wresp.Events) {
					t.Errorf("#%d.%d: resp.Events got = %+v, want = %+v", i, j, resp.Events, wresp.Events)
				}
			}

			rok, nr := waitResponse(wStream, 1*time.Second)
			if !rok {
				t.Errorf("unexpected pb.WatchResponse is received %+v", nr)
			}

			// wait for the client to finish sending the keys before terminating the cluster
			<-ch
		})
	}
}

// TestV3WatchFutureRevision tests Watch APIs from a future revision.
func TestV3WatchFutureRevision(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wStream, err := client.Watch(ctx)
	if err != nil {
		t.Fatalf("wAPI.Watch error: %v", err)
	}

	wkey := []byte("foo")
	wrev := int64(20)
	req := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: wkey, StartRevision: wrev},
	}}
	err = wStream.Send(req)
	if err != nil {
		t.Fatalf("wStream.Send error: %v", err)
	}

	// ensure watcher request created a new watcher
	cresp, err := wStream.Recv()
	if err != nil {
		t.Fatalf("wStream.Recv error: %v", err)
	}
	if !cresp.Created {
		t.Fatalf("create %v, want %v", cresp.Created, true)
	}

	for {
		req := &pb.PutRequest{Key: wkey, Value: []byte("bar")}
		resp, rerr := client.Put(context.TODO(), req)
		if rerr != nil {
			t.Fatalf("couldn't put key (%v)", rerr)
			time.Sleep(time.Duration(1000000000))
		}
		if resp.Header.Revision == wrev {
			break
		}
	}

	// ensure watcher request created a new watcher
	cresp, err = wStream.Recv()
	if err != nil {
		t.Fatalf("wStream.Recv error: %v", err)
	}
	if cresp.Header.Revision != wrev {
		t.Fatalf("revision = %d, want %d", cresp.Header.Revision, wrev)
	}
	if len(cresp.Events) != 1 {
		t.Fatalf("failed to receive events")
	}
	if cresp.Events[0].Kv.ModRevision != wrev {
		t.Errorf("mod revision = %d, want %d", cresp.Events[0].Kv.ModRevision, wrev)
	}
}

// TestV3WatchWrongRange tests wrong range does not create watchers.
func TestV3WatchWrongRange(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wStream, err := client.Watch(ctx)
	if err != nil {
		t.Fatalf("wAPI.Watch error: %v", err)
	}

	tests := []struct {
		key      []byte
		end      []byte
		canceled bool
	}{
		{[]byte("a"), []byte("a"), true},  // wrong range end
		{[]byte("b"), []byte("a"), true},  // wrong range end
		{[]byte("foo"), []byte{0}, false}, // watch request with 'WithFromKey'
	}
	for i, tt := range tests {
		if err := wStream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{Key: tt.key, RangeEnd: tt.end, StartRevision: 1},
		}}); err != nil {
			t.Fatalf("#%d: wStream.Send error: %v", i, err)
		}
		cresp, err := wStream.Recv()
		if err != nil {
			t.Fatalf("#%d: wStream.Recv error: %v", i, err)
		}
		if !cresp.Created {
			t.Fatalf("#%d: create %v, want %v", i, cresp.Created, true)
		}
		if cresp.Canceled != tt.canceled {
			t.Fatalf("#%d: canceled %v, want %v", i, tt.canceled, cresp.Canceled)
		}
		if tt.canceled && cresp.WatchId != -1 {
			t.Fatalf("#%d: canceled watch ID %d, want %d", i, cresp.WatchId, -1)
		}
	}
}

func TestV3WatchCancel(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wStream, errW := client.Watch(ctx)
	if errW != nil {
		t.Fatalf("wAPI.Watch error: %v", errW)
	}

	wreq := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{
			Key: []byte("foo"),
		},
	}}
	if err := wStream.Send(wreq); err != nil {
		t.Fatalf("wStream.Send error: %v", err)
	}

	wresp, errR := wStream.Recv()
	if errR != nil {
		t.Errorf("wStream.Recv error: %v", errR)
	}
	if !wresp.Created {
		t.Errorf("wresp.Created got = %v, want = true", wresp.Created)
	}

	creq := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{
		CancelRequest: &pb.WatchCancelRequest{
			WatchId: wresp.WatchId,
		},
	}}
	if err := wStream.Send(creq); err != nil {
		t.Fatalf("wStream.Send error: %v", err)
	}

	cresp, err := wStream.Recv()
	if err != nil {
		t.Errorf("wStream.Recv error: %v", err)
	}
	if !cresp.Canceled {
		t.Errorf("cresp.Canceled got = %v, want = true", cresp.Canceled)
	}

	if _, err := client.Put(context.TODO(), &pb.PutRequest{Key: []byte("foo"), Value: []byte("bar")}); err != nil {
		t.Errorf("couldn't put key (%v)", err)
	}

	// watch got canceled, so this should block
	rok, nr := waitResponse(wStream, 1*time.Second)
	if !rok {
		t.Errorf("unexpected pb.WatchResponse is received %+v", nr)
	}
}

func TestV3WatchMultipleWatchers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	defer cancel()
	wStream, errW := client.Watch(ctx)
	if errW != nil {
		t.Fatalf("wAPI.Watch error: %v", errW)
	}

	watchKeyN := 4
	for i := 0; i < watchKeyN+1; i++ {
		var wreq *pb.WatchRequest
		if i < watchKeyN {
			wreq = &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("foo"),
				},
			}}
		} else {
			wreq = &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("fo"), RangeEnd: []byte("fp"),
				},
			}}
		}
		if err := wStream.Send(wreq); err != nil {
			t.Fatalf("wStream.Send error: %v", err)
		}
	}

	ids := make(map[int64]struct{})
	for i := 0; i < watchKeyN+1; i++ {
		wresp, err := wStream.Recv()
		if err != nil {
			t.Fatalf("wStream.Recv error: %v", err)
		}
		if !wresp.Created {
			t.Fatalf("wresp.Created got = %v, want = true", wresp.Created)
		}
		ids[wresp.WatchId] = struct{}{}
	}

	if _, err := client.Put(context.TODO(), &pb.PutRequest{Key: []byte("foo"), Value: []byte("bar")}); err != nil {
		t.Fatalf("couldn't put key (%v)", err)
	}

	for i := 0; i < watchKeyN+1; i++ {
		wresp, err := wStream.Recv()
		if err != nil {
			t.Fatalf("wStream.Recv error: %v", err)
		}
		if _, ok := ids[wresp.WatchId]; !ok {
			t.Errorf("watchId %d is not created!", wresp.WatchId)
		} else {
			delete(ids, wresp.WatchId)
		}
		if len(wresp.Events) == 0 {
			t.Errorf("#%d: no events received", i)
		}
		for _, ev := range wresp.Events {
			if string(ev.Kv.Key) != "foo" {
				t.Errorf("ev.Kv.Key got = %s, want = foo", ev.Kv.Key)
			}
			if string(ev.Kv.Value) != "bar" {
				t.Errorf("ev.Kv.Value got = %s, want = bar", ev.Kv.Value)
			}
		}
	}

	// now put one key that has only one matching watcher
	if _, err := client.Put(context.TODO(), &pb.PutRequest{Key: []byte("fo"), Value: []byte("bar")}); err != nil {
		t.Fatalf("couldn't put key (%v)", err)
	}
	wresp, err := wStream.Recv()
	if err != nil {
		t.Errorf("wStream.Recv error: %v", err)
	}
	if len(wresp.Events) != 1 {
		t.Fatalf("len(wresp.Events) got = %d, want = 1", len(wresp.Events))
	}
	if string(wresp.Events[0].Kv.Key) != "fo" {
		t.Errorf("wresp.Events[0].Kv.Key got = %s, want = fo", wresp.Events[0].Kv.Key)
	}

	// now Recv should block because there is no more events coming
	rok, nr := waitResponse(wStream, 1*time.Second)
	if !rok {
		t.Errorf("unexpected pb.WatchResponse is received %+v", nr)
	}
}

func TestV3WatchMultipleStreams(t *testing.T) {

	wAPI := client

	streams := make([]pb.Watch_WatchClient, 5)
	for i := range streams {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		wStream, errW := wAPI.Watch(ctx)
		if errW != nil {
			t.Fatalf("wAPI.Watch error: %v", errW)
		}
		wreq := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("foo"),
			},
		}}
		if err := wStream.Send(wreq); err != nil {
			t.Fatalf("wStream.Send error: %v", err)
		}
		streams[i] = wStream
	}

	for _, wStream := range streams {
		wresp, err := wStream.Recv()
		if err != nil {
			t.Fatalf("wStream.Recv error: %v", err)
		}
		if !wresp.Created {
			t.Fatalf("wresp.Created got = %v, want = true", wresp.Created)
		}
	}

	if _, err := client.Put(context.TODO(), &pb.PutRequest{Key: []byte("foo"), Value: []byte("bar")}); err != nil {
		t.Fatalf("couldn't put key (%v)", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(streams))
	wevents := []*mvccpb.Event{
		{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("foo"), Value: []byte("bar"), Version: 20, ModRevision: 24, CreateRevision: 2},
		},
	}
	for i := range streams {
		go func(i int) {
			defer wg.Done()
			wStream := streams[i]
			wresp, err := wStream.Recv()
			if err != nil {
				t.Errorf("wStream.Recv error: %v", err)
			}
			if !reflect.DeepEqual(wresp.Events, wevents) {
				t.Errorf("wresp.Events got = %+v, want = %+v", wresp.Events, wevents)
			}
			// now Recv should block because there is no more events coming
			rok, nr := waitResponse(wStream, 1*time.Second)
			if !rok {
				t.Errorf("unexpected pb.WatchResponse is received %+v", nr)
			}
		}(i)
	}
	wg.Wait()
}

// waitResponse waits on the given stream for given duration.
// If there is no more events, true and a nil response will be
// returned closing the WatchClient stream. Or the response will
// be returned.
func waitResponse(wc pb.Watch_WatchClient, timeout time.Duration) (bool, *pb.WatchResponse) {
	rCh := make(chan *pb.WatchResponse, 1)
	donec := make(chan struct{})
	defer close(donec)
	go func() {
		resp, _ := wc.Recv()
		select {
		case rCh <- resp:
		case <-donec:
		}
	}()
	select {
	case nr := <-rCh:
		return false, nr
	case <-time.After(timeout):
	}
	// didn't get response
	wc.CloseSend()
	return true, nil
}

func TestWatchWithProgressNotify(t *testing.T) {
	// accelerate report interval so test terminates quickly
	// using atomics to avoid race warnings
	testInterval := 30 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wStream, wErr := client.Watch(ctx)
	if wErr != nil {
		t.Fatalf("wAPI.Watch error: %v", wErr)
	}

	// create two watchers, one with progressNotify set.
	wreq := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("foo"), ProgressNotify: true},
	}}
	if err := wStream.Send(wreq); err != nil {
		t.Fatalf("watch request failed (%v)", err)
	}
	wreq = &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("foo")},
	}}
	if err := wStream.Send(wreq); err != nil {
		t.Fatalf("watch request failed (%v)", err)
	}

	// two creation  + one notification
	for i := 0; i < 3; i++ {
		rok, resp := waitResponse(wStream, testInterval+time.Second)
		if resp.Created {
			continue
		}

		if rok {
			t.Errorf("failed to receive response from watch stream")
		}
		if len(resp.Events) != 0 {
			t.Errorf("len(resp.Events) = %d, want 0", len(resp.Events))
		}
	}

	// no more notification
	rok, resp := waitResponse(wStream, time.Second)
	if !rok {
		t.Errorf("unexpected pb.WatchResponse is received %+v", resp)
	}
}

// TestV3WatchClose opens many watchers concurrently on multiple streams.
func TestV3WatchClose(t *testing.T) {

	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			ctx, cancel := context.WithCancel(context.TODO())
			defer func() {
				wg.Done()
				cancel()
			}()
			ws, err := client.Watch(ctx)
			if err != nil {
				return
			}
			cr := &pb.WatchCreateRequest{Key: []byte("a")}
			req := &pb.WatchRequest{
				RequestUnion: &pb.WatchRequest_CreateRequest{
					CreateRequest: cr,
				},
			}
			ws.Send(req)
			ws.Recv()
		}()
	}

	wg.Wait()
}

// TestV3WatchWithFilter ensures watcher filters out the events correctly.
func TestV3WatchWithFilter(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ws, werr := client.Watch(ctx)
	require.NoError(t, werr)
	req := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{
			Key:     []byte("foo"),
			Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NOPUT},
		},
	}}
	require.NoError(t, ws.Send(req))
	_, err := ws.Recv()
	require.NoError(t, err)

	recv := make(chan *pb.WatchResponse, 1)
	go func() {
		// check received PUT
		resp, rerr := ws.Recv()
		if rerr != nil {
			t.Error(rerr)
		}
		recv <- resp
	}()

	preq := &pb.PutRequest{Key: []byte("foo"), Value: []byte("val")}
	_, err = client.Put(context.TODO(), preq)
	require.NoError(t, err)

	select {
	case <-recv:
		t.Fatal("failed to filter out put event")
	case <-time.After(100 * time.Millisecond):
	}

	dreq := &pb.DeleteRangeRequest{Key: []byte("foo")}
	_, err = client.Delete(context.TODO(), dreq)
	require.NoError(t, err)

	select {
	case resp := <-recv:
		wevs := []*mvccpb.Event{
			{
				Type: mvccpb.DELETE,
				Kv:   &mvccpb.KeyValue{Key: []byte("foo"), ModRevision: 26},
			},
		}
		if !reflect.DeepEqual(resp.Events, wevs) {
			t.Fatalf("got %v, expected %v", resp.Events, wevs)
		}
	case <-time.After(5000 * time.Millisecond):
		t.Fatal("failed to receive delete event")
	}
}

func TestV3WatchWithPrevKV(t *testing.T) {
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()

	tests := []struct {
		key  string
		end  string
		vals []string
	}{{
		key:  "foo",
		end:  "fop",
		vals: []string{"bar1", "bar2"},
	}, {
		key:  "/abc",
		end:  "/abd",
		vals: []string{"first", "second"},
	}}
	for i, tt := range tests {
		_, err := client.Put(context.TODO(), &pb.PutRequest{Key: []byte(tt.key), Value: []byte(tt.vals[0])})
		require.NoError(t, err)

		ws, werr := client.Watch(wctx)
		require.NoError(t, werr)

		req := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:      []byte(tt.key),
				RangeEnd: []byte(tt.end),
				PrevKv:   true,
			},
		}}
		err = ws.Send(req)
		require.NoError(t, err)
		_, err = ws.Recv()
		require.NoError(t, err)

		_, err = client.Put(context.TODO(), &pb.PutRequest{Key: []byte(tt.key), Value: []byte(tt.vals[1])})
		require.NoError(t, err)

		recv := make(chan *pb.WatchResponse, 1)
		go func() {
			// check received PUT
			resp, rerr := ws.Recv()
			if rerr != nil {
				t.Error(rerr)
			}
			recv <- resp
		}()

		select {
		case resp := <-recv:
			if tt.vals[1] != string(resp.Events[0].Kv.Value) {
				t.Errorf("#%d: unequal value: want=%s, get=%s", i, tt.vals[1], resp.Events[0].Kv.Value)
			}
			if tt.vals[0] != string(resp.Events[0].PrevKv.Value) {
				t.Errorf("#%d: unequal value: want=%s, get=%s", i, tt.vals[0], resp.Events[0].PrevKv.Value)
			}

		case <-time.After(30 * time.Second):
			t.Error("timeout waiting for watch response")
		}
	}
}
