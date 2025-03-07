// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/mock/gomock"
)

func getCSVParser(ctx context.Context, t *testing.T, fileName string) mydump.Parser {
	file, err := os.Open(fileName)
	require.NoError(t, err)
	csvParser, err := mydump.NewCSVParser(ctx, &config.CSVConfig{Separator: `,`, Delimiter: `"`},
		file, importer.LoadDataReadBlockSize, nil, false, nil)
	require.NoError(t, err)
	return csvParser
}

func TestFileChunkProcess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tempDir := t.TempDir()

	stmt := "create table test.t(a int, b int, c int, key(a), key(b,c))"
	tk.MustExec(stmt)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	fieldMappings := make([]*importer.FieldMapping, 0, len(table.VisibleCols()))
	for _, v := range table.VisibleCols() {
		fieldMapping := &importer.FieldMapping{
			Column: v,
		}
		fieldMappings = append(fieldMappings, fieldMapping)
	}
	logger := log.L()
	mode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	encoder, err := importer.NewTableKVEncoder(
		&encode.EncodingConfig{
			SessionOptions: encode.SessionOptions{
				SQLMode: mode,
			},
			Table:  table,
			Logger: logger,
		},
		&importer.TableImporter{
			LoadDataController: &importer.LoadDataController{
				ASTArgs:       &importer.ASTArgs{},
				InsertColumns: table.VisibleCols(),
				FieldMappings: fieldMappings,
			},
		},
	)
	require.NoError(t, err)
	diskQuotaLock := &syncutil.RWMutex{}
	codec := tikv.NewCodecV1(tikv.ModeRaw)

	t.Run("process success", func(t *testing.T) {
		var dataKVCnt, indexKVCnt int
		fileName := path.Join(tempDir, "test.csv")
		sourceData := []byte("1,2,3\n4,5,6\n7,8,9\n")
		require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
		csvParser := getCSVParser(ctx, t, fileName)
		defer func() {
			require.NoError(t, csvParser.Close())
		}()
		metrics := tidbmetrics.GetRegisteredImportMetrics(promutil.NewDefaultFactory(),
			prometheus.Labels{
				proto.TaskIDLabelName: uuid.New().String(),
			})
		ctx = metric.WithCommonMetric(ctx, metrics)
		defer func() {
			tidbmetrics.UnregisterImportMetrics(metrics)
		}()
		bak := importer.MinDeliverRowCnt
		importer.MinDeliverRowCnt = 2
		defer func() {
			importer.MinDeliverRowCnt = bak
		}()

		dataWriter := mock.NewMockEngineWriter(ctrl)
		indexWriter := mock.NewMockEngineWriter(ctrl)
		dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, columnNames []string, rows encode.Rows) error {
				dataKVCnt += len(rows.(*kv.Pairs).Pairs)
				return nil
			}).AnyTimes()
		indexWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, columnNames []string, rows encode.Rows) error {
				indexKVCnt += len(rows.(*kv.Pairs).Pairs)
				return nil
			}).AnyTimes()

		chunkInfo := &checkpoints.ChunkCheckpoint{
			Chunk: mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		processor := importer.NewFileChunkProcessor(
			csvParser, encoder, codec,
			chunkInfo, logger.Logger, diskQuotaLock, dataWriter, indexWriter,
		)
		require.NoError(t, processor.Process(ctx))
		require.True(t, ctrl.Satisfied())
		require.Equal(t, uint64(9), chunkInfo.Checksum.SumKVS())
		require.Equal(t, uint64(348), chunkInfo.Checksum.SumSize())
		require.Equal(t, 3, dataKVCnt)
		require.Equal(t, 6, indexKVCnt)
		require.Equal(t, float64(len(sourceData)), metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.StateRestored)))
		require.Equal(t, float64(3), metric.ReadCounter(metrics.RowsCounter.WithLabelValues(metric.StateRestored, "")))
		require.Equal(t, uint64(2), *metric.ReadHistogram(metrics.RowEncodeSecondsHistogram).Histogram.SampleCount)
		require.Equal(t, uint64(2), *metric.ReadHistogram(metrics.RowReadSecondsHistogram).Histogram.SampleCount)
	})

	t.Run("encode error", func(t *testing.T) {
		fileName := path.Join(tempDir, "test.csv")
		sourceData := []byte(`1,2,3\n4,aa,6\n7,8,9\n`)
		require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
		csvParser := getCSVParser(ctx, t, fileName)
		defer func() {
			require.NoError(t, csvParser.Close())
		}()

		dataWriter := mock.NewMockEngineWriter(ctrl)
		indexWriter := mock.NewMockEngineWriter(ctrl)
		dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		indexWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		chunkInfo := &checkpoints.ChunkCheckpoint{
			Chunk: mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		processor := importer.NewFileChunkProcessor(
			csvParser, encoder, codec,
			chunkInfo, logger.Logger, diskQuotaLock, dataWriter, indexWriter,
		)
		require.ErrorIs(t, processor.Process(ctx), common.ErrEncodeKV)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("invalid file", func(t *testing.T) {
		fileName := path.Join(tempDir, "test.csv")
		sourceData := []byte(`1,"`)
		require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
		csvParser := getCSVParser(ctx, t, fileName)
		defer func() {
			require.NoError(t, csvParser.Close())
		}()

		dataWriter := mock.NewMockEngineWriter(ctrl)
		indexWriter := mock.NewMockEngineWriter(ctrl)
		dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		indexWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		chunkInfo := &checkpoints.ChunkCheckpoint{
			Chunk: mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		processor := importer.NewFileChunkProcessor(
			csvParser, encoder, codec,
			chunkInfo, logger.Logger, diskQuotaLock, dataWriter, indexWriter,
		)
		require.ErrorIs(t, processor.Process(ctx), common.ErrEncodeKV)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("data KV write error", func(t *testing.T) {
		fileName := path.Join(tempDir, "test.csv")
		sourceData := []byte("1,2,3\n4,5,6\n7,8,9\n")
		require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
		csvParser := getCSVParser(ctx, t, fileName)
		defer func() {
			require.NoError(t, csvParser.Close())
		}()

		dataWriter := mock.NewMockEngineWriter(ctrl)
		indexWriter := mock.NewMockEngineWriter(ctrl)
		dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("data write error"))

		chunkInfo := &checkpoints.ChunkCheckpoint{
			Chunk: mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		processor := importer.NewFileChunkProcessor(
			csvParser, encoder, codec,
			chunkInfo, logger.Logger, diskQuotaLock, dataWriter, indexWriter,
		)
		require.ErrorContains(t, processor.Process(ctx), "data write error")
		require.True(t, ctrl.Satisfied())
	})

	t.Run("index KV write error", func(t *testing.T) {
		fileName := path.Join(tempDir, "test.csv")
		sourceData := []byte("1,2,3\n4,5,6\n7,8,9\n")
		require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
		csvParser := getCSVParser(ctx, t, fileName)
		defer func() {
			require.NoError(t, csvParser.Close())
		}()

		dataWriter := mock.NewMockEngineWriter(ctrl)
		indexWriter := mock.NewMockEngineWriter(ctrl)
		dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		indexWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("index write error"))

		chunkInfo := &checkpoints.ChunkCheckpoint{
			Chunk: mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		processor := importer.NewFileChunkProcessor(
			csvParser, encoder, codec,
			chunkInfo, logger.Logger, diskQuotaLock, dataWriter, indexWriter,
		)
		require.ErrorContains(t, processor.Process(ctx), "index write error")
		require.True(t, ctrl.Satisfied())
	})
}
