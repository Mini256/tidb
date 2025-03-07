// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, []byte) ([]byte, error) {
		return nil, errors.New("failed")
	}
	schedulerPauseCh := make(chan struct{})
	pdController := &PdController{addrs: []string{"", ""}, schedulerPauseCh: schedulerPauseCh}
	// As pdController.Client is nil, (*pdController).Close() can not be called directly.
	defer close(schedulerPauseCh)

	_, err := pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, nil, mock)
	require.EqualError(t, err, "failed")

	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	cfg := map[string]interface{}{
		"max-merge-region-keys":       0,
		"max-snapshot":                1,
		"enable-location-replacement": false,
		"max-pending-peer-count":      uint64(16),
	}
	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{}, cfg, mock)
	require.Error(t, err)
	require.Regexp(t, "^failed to update PD", err.Error())
	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	_, err = pdController.listSchedulersWith(ctx, mock)
	require.EqualError(t, err, "failed")

	mock = func(context.Context, string, string, *http.Client, string, []byte) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}

	_, err = pdController.pauseSchedulersAndConfigWith(ctx, []string{scheduler}, cfg, mock)
	require.NoError(t, err)

	// pauseSchedulersAndConfigWith will wait on chan schedulerPauseCh
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	require.NoError(t, err)

	schedulers, err := pdController.listSchedulersWith(ctx, mock)
	require.NoError(t, err)
	require.Len(t, schedulers, 1)
	require.Equal(t, scheduler, schedulers[0])
}

func TestGetClusterVersion(t *testing.T) {
	pdController := &PdController{addrs: []string{"", ""}} // two endpoints
	counter := 0
	mock := func(context.Context, string, string, *http.Client, string, []byte) ([]byte, error) {
		counter++
		if counter <= 1 {
			return nil, errors.New("mock error")
		}
		return []byte(`test`), nil
	}

	ctx := context.Background()
	respString, err := pdController.getClusterVersionWith(ctx, mock)
	require.NoError(t, err)
	require.Equal(t, "test", respString)

	mock = func(context.Context, string, string, *http.Client, string, []byte) ([]byte, error) {
		return nil, errors.New("mock error")
	}
	_, err = pdController.getClusterVersionWith(ctx, mock)
	require.Error(t, err)
}

func TestRegionCount(t *testing.T) {
	regions := &pdtypes.RegionTree{}
	regions.SetRegion(pdtypes.NewRegionInfo(&metapb.Region{
		Id:          1,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 1}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 3}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(pdtypes.NewRegionInfo(&metapb.Region{
		Id:          2,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 5}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(pdtypes.NewRegionInfo(&metapb.Region{
		Id:          3,
		StartKey:    codec.EncodeBytes(nil, []byte{2, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{3, 4}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	require.Equal(t, 3, len(regions.Regions))

	mock := func(
		_ context.Context, addr string, prefix string, _ *http.Client, _ string, _ []byte,
	) ([]byte, error) {
		query := fmt.Sprintf("%s/%s", addr, prefix)
		u, e := url.Parse(query)
		require.NoError(t, e, query)
		start := u.Query().Get("start_key")
		end := u.Query().Get("end_key")
		t.Log(hex.EncodeToString([]byte(start)))
		t.Log(hex.EncodeToString([]byte(end)))
		scanRegions := regions.ScanRange([]byte(start), []byte(end), 0)
		stats := pdtypes.RegionStats{Count: len(scanRegions)}
		ret, err := json.Marshal(stats)
		require.NoError(t, err)
		return ret, nil
	}

	pdController := &PdController{addrs: []string{"http://mock"}}
	ctx := context.Background()
	resp, err := pdController.getRegionCountWith(ctx, mock, []byte{}, []byte{})
	require.NoError(t, err)
	require.Equal(t, 3, resp)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{0}, []byte{0xff})
	require.NoError(t, err)
	require.Equal(t, 3, resp)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{1, 2}, []byte{1, 4})
	require.NoError(t, err)
	require.Equal(t, 2, resp)
}

func TestPDVersion(t *testing.T) {
	v := []byte("\"v4.1.0-alpha1\"\n")
	r := parseVersion(v)
	expectV := semver.New("4.1.0-alpha1")
	require.Equal(t, expectV.Major, r.Major)
	require.Equal(t, expectV.Minor, r.Minor)
	require.Equal(t, expectV.PreRelease, r.PreRelease)
}

func TestPDRequestRetry(t *testing.T) {
	ctx := context.Background()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/FastRetry", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/pdutil/FastRetry"))
	}()

	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		bytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "test", string(bytes))
		if count <= PDRequestRetryTime-1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	cli := http.DefaultClient
	cli.Transport = http.DefaultTransport.(*http.Transport).Clone()
	// although the real code doesn't disable keep alive, we need to disable it
	// in test to avoid the connection being reused and #47930 can't appear. The
	// real code will only meet #47930 when go's internal http client just dropped
	// all idle connections.
	cli.Transport.(*http.Transport).DisableKeepAlives = true

	taddr := ts.URL
	_, reqErr := pdRequest(ctx, taddr, "", cli, http.MethodPost, []byte("test"))
	require.NoError(t, reqErr)
	ts.Close()
	count = 0
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= PDRequestRetryTime+1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.Error(t, reqErr)
	ts.Close()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/InjectClosed",
		fmt.Sprintf("return(%d)", 0)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/pdutil/InjectClosed"))
	}()
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.NoError(t, reqErr)
	ts.Close()

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/pdutil/InjectClosed"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/InjectClosed",
		fmt.Sprintf("return(%d)", 1)))
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.NoError(t, reqErr)
}

func TestPDResetTSCompatibility(t *testing.T) {
	ctx := context.Background()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()
	pd := PdController{addrs: []string{ts.URL}, cli: http.DefaultClient}
	reqErr := pd.ResetTS(ctx, 123)
	require.NoError(t, reqErr)

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts2.Close()
	pd = PdController{addrs: []string{ts2.URL}, cli: http.DefaultClient}
	reqErr = pd.ResetTS(ctx, 123)
	require.NoError(t, reqErr)
}

func TestStoreInfo(t *testing.T) {
	storeInfo := pdtypes.StoreInfo{
		Status: &pdtypes.StoreStatus{
			Capacity:  pdtypes.ByteSize(1024),
			Available: pdtypes.ByteSize(1024),
		},
		Store: &pdtypes.MetaStore{
			StateName: "Tombstone",
		},
	}
	mock := func(
		_ context.Context, addr string, prefix string, _ *http.Client, _ string, _ []byte,
	) ([]byte, error) {
		require.Equal(t,
			fmt.Sprintf("http://mock%s", pdhttp.StoreByID(1)),
			fmt.Sprintf("%s%s", addr, prefix))
		ret, err := json.Marshal(storeInfo)
		require.NoError(t, err)
		return ret, nil
	}

	pdController := &PdController{addrs: []string{"http://mock"}}
	ctx := context.Background()
	resp, err := pdController.getStoreInfoWith(ctx, mock, 1)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Status)
	require.Equal(t, "Tombstone", resp.Store.StateName)
	require.Equal(t, uint64(1024), uint64(resp.Status.Available))
}

func TestPauseSchedulersByKeyRange(t *testing.T) {
	const ttl = time.Second

	labelExpires := make(map[string]time.Time)

	var (
		mu      sync.Mutex
		deleted bool
	)

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if deleted {
			return
		}
		switch r.Method {
		case http.MethodPatch:
			var patch pdhttp.LabelRulePatch
			err := json.NewDecoder(r.Body).Decode(&patch)
			require.NoError(t, err)
			require.Len(t, patch.SetRules, 0)
			require.Len(t, patch.DeleteRules, 1)
			delete(labelExpires, patch.DeleteRules[0])
			deleted = true
		case http.MethodPost:
			var labelRule LabelRule
			err := json.NewDecoder(r.Body).Decode(&labelRule)
			require.NoError(t, err)
			require.Len(t, labelRule.Labels, 1)
			regionLabel := labelRule.Labels[0]
			require.Equal(t, "schedule", regionLabel.Key)
			require.Equal(t, "deny", regionLabel.Value)
			reqTTL, err := time.ParseDuration(regionLabel.TTL)
			require.NoError(t, err)
			if reqTTL == 0 {
				delete(labelExpires, labelRule.ID)
			} else {
				require.Equal(t, ttl, reqTTL)
				if expire, ok := labelExpires[labelRule.ID]; ok {
					require.True(t, expire.After(time.Now()), "should not expire before now")
				}
				labelExpires[labelRule.ID] = time.Now().Add(ttl)
			}
		}
	}))
	defer httpSrv.Close()

	pdHTTPCli := pdhttp.NewClient("test", []string{httpSrv.URL})
	defer pdHTTPCli.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, err := pauseSchedulerByKeyRangeWithTTL(ctx, pdHTTPCli, []byte{0, 0, 0, 0}, []byte{0xff, 0xff, 0xff, 0xff}, ttl)
	require.NoError(t, err)
	time.Sleep(ttl * 3)
	cancel()
	<-done
	require.Len(t, labelExpires, 0)
}
