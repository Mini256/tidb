// Copyright 2017 PingCAP, Inc.
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

package stmtctx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/linter/constructor"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/nocopy"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
	// WarnLevelNote represents level "Note" for 'SHOW WARNINGS' syntax.
	WarnLevelNote = "Note"
)

var taskIDAlloc uint64

// AllocateTaskID allocates a new unique ID for a statement execution
func AllocateTaskID() uint64 {
	return atomic.AddUint64(&taskIDAlloc, 1)
}

// SQLWarn relates a sql warning and it's level.
type SQLWarn struct {
	Level string
	Err   error
}

type jsonSQLWarn struct {
	Level  string        `json:"level"`
	SQLErr *terror.Error `json:"err,omitempty"`
	Msg    string        `json:"msg,omitempty"`
}

// MarshalJSON implements the Marshaler.MarshalJSON interface.
func (warn *SQLWarn) MarshalJSON() ([]byte, error) {
	w := &jsonSQLWarn{
		Level: warn.Level,
	}
	e := errors.Cause(warn.Err)
	switch x := e.(type) {
	case *terror.Error:
		// Omit outter errors because only the most inner error matters.
		w.SQLErr = x
	default:
		w.Msg = e.Error()
	}
	return json.Marshal(w)
}

// UnmarshalJSON implements the Unmarshaler.UnmarshalJSON interface.
func (warn *SQLWarn) UnmarshalJSON(data []byte) error {
	var w jsonSQLWarn
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	warn.Level = w.Level
	if w.SQLErr != nil {
		warn.Err = w.SQLErr
	} else {
		warn.Err = errors.New(w.Msg)
	}
	return nil
}

// ReferenceCount indicates the reference count of StmtCtx.
type ReferenceCount int32

const (
	// ReferenceCountIsFrozen indicates the current StmtCtx is resetting, it'll refuse all the access from other sessions.
	ReferenceCountIsFrozen int32 = -1
	// ReferenceCountNoReference indicates the current StmtCtx is not accessed by other sessions.
	ReferenceCountNoReference int32 = 0
)

// TryIncrease tries to increase the reference count.
// There is a small chance that TryIncrease returns true while TryFreeze and
// UnFreeze are invoked successfully during the execution of TryIncrease.
func (rf *ReferenceCount) TryIncrease() bool {
	refCnt := atomic.LoadInt32((*int32)(rf))
	for ; refCnt != ReferenceCountIsFrozen && !atomic.CompareAndSwapInt32((*int32)(rf), refCnt, refCnt+1); refCnt = atomic.LoadInt32((*int32)(rf)) {
	}
	return refCnt != ReferenceCountIsFrozen
}

// Decrease decreases the reference count.
func (rf *ReferenceCount) Decrease() {
	for refCnt := atomic.LoadInt32((*int32)(rf)); !atomic.CompareAndSwapInt32((*int32)(rf), refCnt, refCnt-1); refCnt = atomic.LoadInt32((*int32)(rf)) {
	}
}

// TryFreeze tries to freeze the StmtCtx to frozen before resetting the old StmtCtx.
func (rf *ReferenceCount) TryFreeze() bool {
	return atomic.LoadInt32((*int32)(rf)) == ReferenceCountNoReference && atomic.CompareAndSwapInt32((*int32)(rf), ReferenceCountNoReference, ReferenceCountIsFrozen)
}

// UnFreeze unfreeze the frozen StmtCtx thus the other session can access this StmtCtx.
func (rf *ReferenceCount) UnFreeze() {
	atomic.StoreInt32((*int32)(rf), ReferenceCountNoReference)
}

var stmtCtxIDGenerator atomic.Uint64

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// NoCopy indicates that this struct cannot be copied because
	// copying this object will make the copied TypeCtx field to refer a wrong `AppendWarnings` func.
	_ nocopy.NoCopy

	_ constructor.Constructor `ctor:"NewStmtCtx,NewStmtCtxWithTimeZone,Reset"`

	ctxID uint64

	// 	typeCtx is used to indicate how to make the type conversation.
	typeCtx types.Context

	// errCtx is used to indicate how to handle the errors
	errCtx errctx.Context

	// Set the following variables before execution
	StmtHints

	// IsDDLJobInQueue is used to mark whether the DDL job is put into the queue.
	// If IsDDLJobInQueue is true, it means the DDL job is in the queue of storage, and it can be handled by the DDL worker.
	IsDDLJobInQueue        bool
	DDLJobID               int64
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	InExplainStmt          bool
	InExplainAnalyzeStmt   bool
	ExplainFormat          string
	InCreateOrAlterStmt    bool
	InSetSessionStatesStmt bool
	InPreparedPlanBuilding bool
	InShowWarning          bool
	UseCache               bool
	ForcePlanCache         bool // force the optimizer to use plan cache even if there is risky optimization, see #49736.
	CacheType              PlanCacheType
	BatchCheck             bool
	InNullRejectCheck      bool
	IgnoreExplainIDSuffix  bool
	MultiSchemaInfo        *model.MultiSchemaInfo
	// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
	// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
	// in stmtCtx
	IsStaleness     bool
	InRestrictedSQL bool
	ViewDepth       int32
	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex

		affectedRows uint64
		foundRows    uint64

		/*
			following variables are ported from 'COPY_INFO' struct of MySQL server source,
			they are used to count rows for INSERT/REPLACE/UPDATE queries:
			  If a row is inserted then the copied variable is incremented.
			  If a row is updated by the INSERT ... ON DUPLICATE KEY UPDATE and the
			     new data differs from the old one then the copied and the updated
			     variables are incremented.
			  The touched variable is incremented if a row was touched by the update part
			     of the INSERT ... ON DUPLICATE KEY UPDATE no matter whether the row
			     was actually changed or not.

			see https://github.com/mysql/mysql-server/blob/d2029238d6d9f648077664e4cdd611e231a6dc14/sql/sql_data_change.h#L60 for more details
		*/
		records uint64
		deleted uint64
		updated uint64
		copied  uint64
		touched uint64

		message  string
		warnings []SQLWarn
		// extraWarnings record the extra warnings and are only used by the slow log only now.
		// If a warning is expected to be output only under some conditions (like in EXPLAIN or EXPLAIN VERBOSE) but it's
		// not under such conditions now, it is considered as an extra warning.
		// extraWarnings would not be printed through SHOW WARNINGS, but we want to always output them through the slow
		// log to help diagnostics, so we store them here separately.
		extraWarnings []SQLWarn

		execDetails    execdetails.ExecDetails
		detailsSummary execdetails.P90Summary
	}
	// PrevAffectedRows is the affected-rows value(DDL is 0, DML is the number of affected rows).
	PrevAffectedRows int64
	// PrevLastInsertID is the last insert ID of previous statement.
	PrevLastInsertID uint64
	// LastInsertID is the auto-generated ID in the current statement.
	LastInsertID uint64
	// InsertID is the given insert ID of an auto_increment column.
	InsertID uint64

	BaseRowID int64
	MaxRowID  int64

	// Copied from SessionVars.TimeZone.
	Priority     mysql.PriorityEnum
	NotFillCache bool
	MemTracker   *memory.Tracker
	DiskTracker  *disk.Tracker
	// per statement resource group name
	// hint /* +ResourceGroup(name) */ can change the statement group name
	ResourceGroupName string
	RunawayChecker    *resourcegroup.RunawayChecker
	IsTiFlash         atomic2.Bool
	RuntimeStatsColl  *execdetails.RuntimeStatsColl
	TableIDs          []int64
	IndexNames        []string
	StmtType          string
	OriginalSQL       string
	digestMemo        struct {
		sync.Once
		normalized string
		digest     *parser.Digest
	}
	// BindSQL used to construct the key for plan cache. It records the binding used by the stmt.
	// If the binding is not used by the stmt, the value is empty
	BindSQL string

	// The several fields below are mainly for some diagnostic features, like stmt summary and slow query.
	// We cache the values here to avoid calculating them multiple times.
	// Note:
	//   Avoid accessing these fields directly, use their Setter/Getter methods instead.
	//   Other fields should be the zero value or be consistent with the plan field.
	// TODO: more clearly distinguish between the value is empty and the value has not been set
	planNormalized string
	planDigest     *parser.Digest
	encodedPlan    string
	planHint       string
	planHintSet    bool
	binaryPlan     string
	// To avoid cycle import, we use interface{} for the following two fields.
	// flatPlan should be a *plannercore.FlatPhysicalPlan if it's not nil
	flatPlan interface{}
	// plan should be a plannercore.Plan if it's not nil
	plan interface{}

	Tables                []TableEntry
	PointExec             bool  // for point update cached execution, Constant expression need to set "paramMarker"
	lockWaitStartTime     int64 // LockWaitStartTime stores the pessimistic lock wait start time
	PessimisticLockWaited int32
	LockKeysDuration      int64
	LockKeysCount         int32
	LockTableIDs          map[int64]struct{} // table IDs need to be locked, empty for lock all tables
	TblInfo2UnionScan     map[*model.TableInfo]bool
	TaskID                uint64 // unique ID for an execution of a statement
	TaskMapBakTS          uint64 // counter for

	// stmtCache is used to store some statement-related values.
	// add mutex to protect stmtCache concurrent access
	// https://github.com/pingcap/tidb/issues/36159
	stmtCache struct {
		mu   sync.Mutex
		data map[StmtCacheKey]interface{}
	}

	// Map to store all CTE storages of current SQL.
	// Will clean up at the end of the execution.
	CTEStorageMap interface{}

	SetVarHintRestore map[string]string

	// If the statement read from table cache, this flag is set.
	ReadFromTableCache bool

	// cache is used to reduce object allocation.
	cache struct {
		execdetails.RuntimeStatsColl
		MemTracker  memory.Tracker
		DiskTracker disk.Tracker
		LogOnExceed [2]memory.LogOnExceed
	}

	// InVerboseExplain indicates the statement is "explain format='verbose' ...".
	InVerboseExplain bool

	// EnableOptimizeTrace indicates whether enable optimizer trace by 'trace plan statement'
	EnableOptimizeTrace bool
	// OptimizeTracer indicates the tracer for optimize
	OptimizeTracer *tracing.OptimizeTracer

	// EnableOptimizerCETrace indicate if cardinality estimation internal process needs to be traced.
	// CE Trace is currently a submodule of the optimizer trace and is controlled by a separated option.
	EnableOptimizerCETrace bool
	OptimizerCETrace       []*tracing.CETraceRecord

	EnableOptimizerDebugTrace bool
	OptimizerDebugTrace       interface{}

	// WaitLockLeaseTime is the duration of cached table read lease expiration time.
	WaitLockLeaseTime time.Duration

	// KvExecCounter is created from SessionVars.StmtStats to count the number of SQL
	// executions of the kv layer during the current execution of the statement.
	// Its life cycle is limited to this execution, and a new KvExecCounter is
	// always created during each statement execution.
	KvExecCounter *stmtstats.KvExecCounter

	// WeakConsistency is true when read consistency is weak and in a read statement and not in a transaction.
	WeakConsistency bool

	StatsLoad struct {
		// Timeout to wait for sync-load
		Timeout time.Duration
		// NeededItems stores the columns/indices whose stats are needed for planner.
		NeededItems []model.TableItemID
		// ResultCh to receive stats loading results
		ResultCh chan StatsLoadResult
		// LoadStartTime is to record the load start time to calculate latency
		LoadStartTime time.Time
	}

	// SysdateIsNow indicates whether sysdate() is an alias of now() in this statement
	SysdateIsNow bool

	// RCCheckTS indicates the current read-consistency read select statement will use `RCCheckTS` path.
	RCCheckTS bool

	// IsSQLRegistered uses to indicate whether the SQL has been registered for TopSQL.
	IsSQLRegistered atomic2.Bool
	// IsSQLAndPlanRegistered uses to indicate whether the SQL and plan has been registered for TopSQL.
	IsSQLAndPlanRegistered atomic2.Bool
	// IsReadOnly uses to indicate whether the SQL is read-only.
	IsReadOnly bool
	// usedStatsInfo records version of stats of each table used in the query.
	// It's a map of table physical id -> *UsedStatsInfoForTable
	usedStatsInfo map[int64]*UsedStatsInfoForTable
	// IsSyncStatsFailed indicates whether any failure happened during sync stats
	IsSyncStatsFailed bool
	// UseDynamicPruneMode indicates whether use UseDynamicPruneMode in query stmt
	UseDynamicPruneMode bool
	// ColRefFromPlan mark the column ref used by assignment in update statement.
	ColRefFromUpdatePlan []int64

	// RangeFallback indicates that building complete ranges exceeds the memory limit so it falls back to less accurate ranges such as full range.
	RangeFallback bool

	// IsExplainAnalyzeDML is true if the statement is "explain analyze DML executors", before responding the explain
	// results to the client, the transaction should be committed first. See issue #37373 for more details.
	IsExplainAnalyzeDML bool

	// InHandleForeignKeyTrigger indicates currently are handling foreign key trigger.
	InHandleForeignKeyTrigger bool

	// ForeignKeyTriggerCtx is the contain information for foreign key cascade execution.
	ForeignKeyTriggerCtx struct {
		// The SavepointName is use to do rollback when handle foreign key cascade failed.
		SavepointName string
		HasFKCascades bool
	}

	// MPPQueryInfo stores some id and timestamp of current MPP query statement.
	MPPQueryInfo struct {
		QueryID              atomic2.Uint64
		QueryTS              atomic2.Uint64
		AllocatedMPPTaskID   atomic2.Int64
		AllocatedMPPGatherID atomic2.Uint64
	}

	// TableStats stores the visited runtime table stats by table id during query
	TableStats map[int64]interface{}
	// useChunkAlloc indicates whether statement use chunk alloc
	useChunkAlloc bool
	// Check if TiFlash read engine is removed due to strict sql mode.
	TiFlashEngineRemovedDueToStrictSQLMode bool
	// StaleTSOProvider is used to provide stale timestamp oracle for read-only transactions.
	StaleTSOProvider struct {
		sync.Mutex
		value *uint64
		eval  func() (uint64, error)
	}
}

var defaultErrLevels = func() (l errctx.LevelMap) {
	l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
	return
}()

// NewStmtCtx creates a new statement context
func NewStmtCtx() *StatementContext {
	return NewStmtCtxWithTimeZone(time.UTC)
}

// NewStmtCtxWithTimeZone creates a new StatementContext with the given timezone
func NewStmtCtxWithTimeZone(tz *time.Location) *StatementContext {
	intest.AssertNotNil(tz)
	sc := &StatementContext{
		ctxID: stmtCtxIDGenerator.Add(1),
	}
	sc.typeCtx = types.NewContext(types.DefaultStmtFlags, tz, sc)
	sc.errCtx = newErrCtx(sc.typeCtx, defaultErrLevels, sc)
	return sc
}

// Reset resets a statement context
func (sc *StatementContext) Reset() {
	*sc = StatementContext{
		ctxID: stmtCtxIDGenerator.Add(1),
	}
	sc.typeCtx = types.NewContext(types.DefaultStmtFlags, time.UTC, sc)
	sc.errCtx = newErrCtx(sc.typeCtx, defaultErrLevels, sc)
}

// CtxID returns the context id of the statement
func (sc *StatementContext) CtxID() uint64 {
	return sc.ctxID
}

// TimeZone returns the timezone of the type context
func (sc *StatementContext) TimeZone() *time.Location {
	intest.AssertNotNil(sc)
	if sc == nil {
		return time.UTC
	}

	return sc.typeCtx.Location()
}

// SetTimeZone sets the timezone
func (sc *StatementContext) SetTimeZone(tz *time.Location) {
	intest.AssertNotNil(tz)
	sc.typeCtx = sc.typeCtx.WithLocation(tz)
}

// TypeCtx returns the type context
func (sc *StatementContext) TypeCtx() types.Context {
	return sc.typeCtx
}

// ErrCtx returns the error context
func (sc *StatementContext) ErrCtx() errctx.Context {
	return sc.errCtx
}

// SetErrLevels sets the error levels for statement
// The argument otherLevels is used to set the error levels except truncate
func (sc *StatementContext) SetErrLevels(otherLevels errctx.LevelMap) {
	sc.errCtx = newErrCtx(sc.typeCtx, otherLevels, sc)
}

// ErrLevels returns the current `errctx.LevelMap`
func (sc *StatementContext) ErrLevels() errctx.LevelMap {
	return sc.errCtx.LevelMap()
}

// ErrGroupLevel returns the error level for the given error group
func (sc *StatementContext) ErrGroupLevel(group errctx.ErrGroup) errctx.Level {
	return sc.errCtx.LevelForGroup(group)
}

// TypeFlags returns the type flags
func (sc *StatementContext) TypeFlags() types.Flags {
	return sc.typeCtx.Flags()
}

// SetTypeFlags sets the type flags
func (sc *StatementContext) SetTypeFlags(flags types.Flags) {
	sc.typeCtx = sc.typeCtx.WithFlags(flags)
	sc.errCtx = newErrCtx(sc.typeCtx, sc.errCtx.LevelMap(), sc)
}

// HandleTruncate ignores or returns the error based on the TypeContext inside.
// TODO: replace this function with `HandleError`, for `TruncatedError` they should have the same effect.
func (sc *StatementContext) HandleTruncate(err error) error {
	return sc.typeCtx.HandleTruncate(err)
}

// HandleError handles the error based on `ErrCtx()`
func (sc *StatementContext) HandleError(err error) error {
	intest.AssertNotNil(sc)
	if sc == nil {
		return err
	}
	errCtx := sc.ErrCtx()
	return errCtx.HandleError(err)
}

// HandleErrorWithAlias handles the error based on `ErrCtx()`
func (sc *StatementContext) HandleErrorWithAlias(internalErr, err, warnErr error) error {
	intest.AssertNotNil(sc)
	if sc == nil {
		return err
	}
	errCtx := sc.ErrCtx()
	return errCtx.HandleErrorWithAlias(internalErr, err, warnErr)
}

// StmtHints are SessionVars related sql hints.
type StmtHints struct {
	// Hint Information
	MemQuotaQuery           int64
	MaxExecutionTime        uint64
	ReplicaRead             byte
	AllowInSubqToJoinAndAgg bool
	NoIndexMergeHint        bool
	StraightJoinOrder       bool
	// EnableCascadesPlanner is use cascades planner for a single query only.
	EnableCascadesPlanner bool
	// ForceNthPlan indicates the PlanCounterTp number for finding physical plan.
	// -1 for disable.
	ForceNthPlan  int64
	ResourceGroup string

	// Hint flags
	HasAllowInSubqToJoinAndAggHint bool
	HasMemQuotaHint                bool
	HasReplicaReadHint             bool
	HasMaxExecutionTime            bool
	HasEnableCascadesPlannerHint   bool
	HasResourceGroup               bool
	SetVars                        map[string]string

	// the original table hints
	OriginalTableHints []*ast.TableOptimizerHint
}

// TaskMapNeedBackUp indicates that whether we need to back up taskMap during physical optimizing.
func (sh *StmtHints) TaskMapNeedBackUp() bool {
	return sh.ForceNthPlan != -1
}

// Clone the StmtHints struct and returns the pointer of the new one.
func (sh *StmtHints) Clone() *StmtHints {
	var (
		vars       map[string]string
		tableHints []*ast.TableOptimizerHint
	)
	if len(sh.SetVars) > 0 {
		vars = make(map[string]string, len(sh.SetVars))
		for k, v := range sh.SetVars {
			vars[k] = v
		}
	}
	if len(sh.OriginalTableHints) > 0 {
		tableHints = make([]*ast.TableOptimizerHint, len(sh.OriginalTableHints))
		copy(tableHints, sh.OriginalTableHints)
	}
	return &StmtHints{
		MemQuotaQuery:                  sh.MemQuotaQuery,
		MaxExecutionTime:               sh.MaxExecutionTime,
		ReplicaRead:                    sh.ReplicaRead,
		AllowInSubqToJoinAndAgg:        sh.AllowInSubqToJoinAndAgg,
		NoIndexMergeHint:               sh.NoIndexMergeHint,
		StraightJoinOrder:              sh.StraightJoinOrder,
		EnableCascadesPlanner:          sh.EnableCascadesPlanner,
		ForceNthPlan:                   sh.ForceNthPlan,
		ResourceGroup:                  sh.ResourceGroup,
		HasAllowInSubqToJoinAndAggHint: sh.HasAllowInSubqToJoinAndAggHint,
		HasMemQuotaHint:                sh.HasMemQuotaHint,
		HasReplicaReadHint:             sh.HasReplicaReadHint,
		HasMaxExecutionTime:            sh.HasMaxExecutionTime,
		HasEnableCascadesPlannerHint:   sh.HasEnableCascadesPlannerHint,
		HasResourceGroup:               sh.HasResourceGroup,
		SetVars:                        vars,
		OriginalTableHints:             tableHints,
	}
}

// StmtCacheKey represents the key type in the StmtCache.
type StmtCacheKey int

const (
	// StmtNowTsCacheKey is a variable for now/current_timestamp calculation/cache of one stmt.
	StmtNowTsCacheKey StmtCacheKey = iota
	// StmtSafeTSCacheKey is a variable for safeTS calculation/cache of one stmt.
	StmtSafeTSCacheKey
	// StmtExternalTSCacheKey is a variable for externalTS calculation/cache of one stmt.
	StmtExternalTSCacheKey
)

// GetOrStoreStmtCache gets the cached value of the given key if it exists, otherwise stores the value.
func (sc *StatementContext) GetOrStoreStmtCache(key StmtCacheKey, value interface{}) interface{} {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	if sc.stmtCache.data == nil {
		sc.stmtCache.data = make(map[StmtCacheKey]interface{})
	}
	if _, ok := sc.stmtCache.data[key]; !ok {
		sc.stmtCache.data[key] = value
	}
	return sc.stmtCache.data[key]
}

// GetOrEvaluateStmtCache gets the cached value of the given key if it exists, otherwise calculate the value.
func (sc *StatementContext) GetOrEvaluateStmtCache(key StmtCacheKey, valueEvaluator func() (interface{}, error)) (interface{}, error) {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	if sc.stmtCache.data == nil {
		sc.stmtCache.data = make(map[StmtCacheKey]interface{})
	}
	if _, ok := sc.stmtCache.data[key]; !ok {
		value, err := valueEvaluator()
		if err != nil {
			return nil, err
		}
		sc.stmtCache.data[key] = value
	}
	return sc.stmtCache.data[key], nil
}

// ResetInStmtCache resets the cache of given key.
func (sc *StatementContext) ResetInStmtCache(key StmtCacheKey) {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	delete(sc.stmtCache.data, key)
}

// ResetStmtCache resets all cached values.
func (sc *StatementContext) ResetStmtCache() {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	sc.stmtCache.data = make(map[StmtCacheKey]interface{})
}

// SQLDigest gets normalized and digest for provided sql.
// it will cache result after first calling.
func (sc *StatementContext) SQLDigest() (normalized string, sqlDigest *parser.Digest) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(sc.OriginalSQL)
	})
	return sc.digestMemo.normalized, sc.digestMemo.digest
}

// InitSQLDigest sets the normalized and digest for sql.
func (sc *StatementContext) InitSQLDigest(normalized string, digest *parser.Digest) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = normalized, digest
	})
}

// ResetSQLDigest sets the normalized and digest for sql anyway, **DO NOT USE THIS UNLESS YOU KNOW WHAT YOU ARE DOING NOW**.
func (sc *StatementContext) ResetSQLDigest(s string) {
	sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(s)
}

// GetPlanDigest gets the normalized plan and plan digest.
func (sc *StatementContext) GetPlanDigest() (normalized string, planDigest *parser.Digest) {
	return sc.planNormalized, sc.planDigest
}

// GetPlan gets the plan field of stmtctx
func (sc *StatementContext) GetPlan() interface{} {
	return sc.plan
}

// SetPlan sets the plan field of stmtctx
func (sc *StatementContext) SetPlan(plan interface{}) {
	sc.plan = plan
}

// GetFlatPlan gets the flatPlan field of stmtctx
func (sc *StatementContext) GetFlatPlan() interface{} {
	return sc.flatPlan
}

// SetFlatPlan sets the flatPlan field of stmtctx
func (sc *StatementContext) SetFlatPlan(flat interface{}) {
	sc.flatPlan = flat
}

// GetBinaryPlan gets the binaryPlan field of stmtctx
func (sc *StatementContext) GetBinaryPlan() string {
	return sc.binaryPlan
}

// SetBinaryPlan sets the binaryPlan field of stmtctx
func (sc *StatementContext) SetBinaryPlan(binaryPlan string) {
	sc.binaryPlan = binaryPlan
}

// GetResourceGroupTagger returns the implementation of tikvrpc.ResourceGroupTagger related to self.
func (sc *StatementContext) GetResourceGroupTagger() tikvrpc.ResourceGroupTagger {
	normalized, digest := sc.SQLDigest()
	planDigest := sc.planDigest
	return func(req *tikvrpc.Request) {
		if req == nil {
			return
		}
		if len(normalized) == 0 {
			return
		}
		req.ResourceGroupTag = resourcegrouptag.EncodeResourceGroupTag(digest, planDigest,
			resourcegrouptag.GetResourceGroupLabelByKey(resourcegrouptag.GetFirstKeyFromRequest(req)))
	}
}

// SetUseChunkAlloc set use chunk alloc status
func (sc *StatementContext) SetUseChunkAlloc() {
	sc.useChunkAlloc = true
}

// ClearUseChunkAlloc clear useChunkAlloc status
func (sc *StatementContext) ClearUseChunkAlloc() {
	sc.useChunkAlloc = false
}

// GetUseChunkAllocStatus returns useChunkAlloc status
func (sc *StatementContext) GetUseChunkAllocStatus() bool {
	return sc.useChunkAlloc
}

// SetPlanDigest sets the normalized plan and plan digest.
func (sc *StatementContext) SetPlanDigest(normalized string, planDigest *parser.Digest) {
	if planDigest != nil {
		sc.planNormalized, sc.planDigest = normalized, planDigest
	}
}

// GetEncodedPlan gets the encoded plan, it is used to avoid repeated encode.
func (sc *StatementContext) GetEncodedPlan() string {
	return sc.encodedPlan
}

// SetEncodedPlan sets the encoded plan, it is used to avoid repeated encode.
func (sc *StatementContext) SetEncodedPlan(encodedPlan string) {
	sc.encodedPlan = encodedPlan
}

// GetPlanHint gets the hint string generated from the plan.
func (sc *StatementContext) GetPlanHint() (string, bool) {
	return sc.planHint, sc.planHintSet
}

// InitDiskTracker initializes the sc.DiskTracker, use cache to avoid allocation.
func (sc *StatementContext) InitDiskTracker(label int, bytesLimit int64) {
	memory.InitTracker(&sc.cache.DiskTracker, label, bytesLimit, &sc.cache.LogOnExceed[0])
	sc.DiskTracker = &sc.cache.DiskTracker
}

// InitMemTracker initializes the sc.MemTracker, use cache to avoid allocation.
func (sc *StatementContext) InitMemTracker(label int, bytesLimit int64) {
	memory.InitTracker(&sc.cache.MemTracker, label, bytesLimit, &sc.cache.LogOnExceed[1])
	sc.MemTracker = &sc.cache.MemTracker
}

// SetPlanHint sets the hint for the plan.
func (sc *StatementContext) SetPlanHint(hint string) {
	sc.planHintSet = true
	sc.planHint = hint
}

// PlanCacheType is the flag of plan cache
type PlanCacheType int

const (
	// DefaultNoCache no cache
	DefaultNoCache PlanCacheType = iota
	// SessionPrepared session prepared plan cache
	SessionPrepared
	// SessionNonPrepared session non-prepared plan cache
	SessionNonPrepared
)

// SetSkipPlanCache sets to skip the plan cache and records the reason.
func (sc *StatementContext) SetSkipPlanCache(reason error) {
	if !sc.UseCache {
		return // avoid unnecessary warnings
	}

	if sc.ForcePlanCache {
		sc.AppendWarning(errors.NewNoStackErrorf("force plan-cache: may use risky cached plan: %s", reason.Error()))
		return
	}
	sc.setSkipPlanCache(reason)
}

// ForceSetSkipPlanCache sets to skip the plan cache and records the reason.
func (sc *StatementContext) ForceSetSkipPlanCache(reason error) {
	if sc.CacheType == DefaultNoCache {
		return
	}
	sc.setSkipPlanCache(reason)
}

func (sc *StatementContext) setSkipPlanCache(reason error) {
	sc.UseCache = false
	switch sc.CacheType {
	case DefaultNoCache:
		sc.AppendWarning(errors.NewNoStackError("unknown cache type"))
	case SessionPrepared:
		sc.AppendWarning(errors.NewNoStackErrorf("skip prepared plan-cache: %s", reason.Error()))
	case SessionNonPrepared:
		if sc.InExplainStmt && sc.ExplainFormat == "plan_cache" {
			// use "plan_cache" rather than types.ExplainFormatPlanCache to avoid import cycle
			sc.AppendWarning(errors.NewNoStackErrorf("skip non-prepared plan-cache: %s", reason.Error()))
		}
	}
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	if sc.InHandleForeignKeyTrigger {
		// For compatibility with MySQL, not add the affected row cause by the foreign key trigger.
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.affectedRows += rows
}

// SetAffectedRows sets affected rows.
func (sc *StatementContext) SetAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.affectedRows = rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.affectedRows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.foundRows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.foundRows += rows
}

// RecordRows is used to generate info message
func (sc *StatementContext) RecordRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.records
}

// AddRecordRows adds record rows.
func (sc *StatementContext) AddRecordRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.records += rows
}

// DeletedRows is used to generate info message
func (sc *StatementContext) DeletedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.deleted
}

// AddDeletedRows adds record rows.
func (sc *StatementContext) AddDeletedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.deleted += rows
}

// UpdatedRows is used to generate info message
func (sc *StatementContext) UpdatedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.updated
}

// AddUpdatedRows adds updated rows.
func (sc *StatementContext) AddUpdatedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.updated += rows
}

// CopiedRows is used to generate info message
func (sc *StatementContext) CopiedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.copied
}

// AddCopiedRows adds copied rows.
func (sc *StatementContext) AddCopiedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.copied += rows
}

// TouchedRows is used to generate info message
func (sc *StatementContext) TouchedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.touched
}

// AddTouchedRows adds touched rows.
func (sc *StatementContext) AddTouchedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.touched += rows
}

// GetMessage returns the extra message of the last executed command, if there is no message, it returns empty string
func (sc *StatementContext) GetMessage() string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.message
}

// SetMessage sets the info message generated by some commands
func (sc *StatementContext) SetMessage(msg string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.message = msg
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.warnings
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (sc *StatementContext) TruncateWarnings(start int) []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sz := len(sc.mu.warnings) - start
	if sz <= 0 {
		return nil
	}
	ret := make([]SQLWarn, sz)
	copy(ret, sc.mu.warnings[start:])
	sc.mu.warnings = sc.mu.warnings[:start]
	return ret
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return uint16(len(sc.mu.warnings))
}

// NumErrorWarnings gets warning and error count.
func (sc *StatementContext) NumErrorWarnings() (ec uint16, wc int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, w := range sc.mu.warnings {
		if w.Level == WarnLevelError {
			ec++
		}
	}
	wc = len(sc.mu.warnings)
	return
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.warnings = warns
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelWarning, warn})
	}
}

// AppendWarnings appends some warnings.
func (sc *StatementContext) AppendWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, warns...)
	}
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelNote, warn})
	}
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelError, warn})
	}
}

// GetExtraWarnings gets extra warnings.
func (sc *StatementContext) GetExtraWarnings() []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.extraWarnings
}

// SetExtraWarnings sets extra warnings.
func (sc *StatementContext) SetExtraWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.extraWarnings = warns
}

// AppendExtraWarning appends an extra warning with level 'Warning'.
func (sc *StatementContext) AppendExtraWarning(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.extraWarnings) < math.MaxUint16 {
		sc.mu.extraWarnings = append(sc.mu.extraWarnings, SQLWarn{WarnLevelWarning, warn})
	}
}

// AppendExtraNote appends an extra warning with level 'Note'.
func (sc *StatementContext) AppendExtraNote(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.extraWarnings) < math.MaxUint16 {
		sc.mu.extraWarnings = append(sc.mu.extraWarnings, SQLWarn{WarnLevelNote, warn})
	}
}

// AppendExtraError appends an extra warning with level 'Error'.
func (sc *StatementContext) AppendExtraError(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.extraWarnings) < math.MaxUint16 {
		sc.mu.extraWarnings = append(sc.mu.extraWarnings, SQLWarn{WarnLevelError, warn})
	}
}

// resetMuForRetry resets the changed states of sc.mu during execution.
func (sc *StatementContext) resetMuForRetry() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.records = 0
	sc.mu.deleted = 0
	sc.mu.updated = 0
	sc.mu.copied = 0
	sc.mu.touched = 0
	sc.mu.message = ""
	sc.mu.warnings = nil
	sc.mu.execDetails = execdetails.ExecDetails{}
	sc.mu.detailsSummary.Reset()
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.resetMuForRetry()
	sc.MaxRowID = 0
	sc.BaseRowID = 0
	sc.TableIDs = sc.TableIDs[:0]
	sc.IndexNames = sc.IndexNames[:0]
	sc.TaskID = AllocateTaskID()
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (sc *StatementContext) MergeExecDetails(details *execdetails.ExecDetails, commitDetails *util.CommitDetails) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if details != nil {
		sc.mu.execDetails.CopTime += details.CopTime
		sc.mu.execDetails.BackoffTime += details.BackoffTime
		sc.mu.execDetails.RequestCount++
		sc.MergeScanDetail(details.ScanDetail)
		sc.MergeTimeDetail(details.TimeDetail)
		detail := &execdetails.DetailsNeedP90{
			BackoffSleep:  details.BackoffSleep,
			BackoffTimes:  details.BackoffTimes,
			CalleeAddress: details.CalleeAddress,
			TimeDetail:    details.TimeDetail,
		}
		sc.mu.detailsSummary.Merge(detail)
	}
	if commitDetails != nil {
		if sc.mu.execDetails.CommitDetail == nil {
			sc.mu.execDetails.CommitDetail = commitDetails
		} else {
			sc.mu.execDetails.CommitDetail.Merge(commitDetails)
		}
	}
}

// MergeScanDetail merges scan details into self.
func (sc *StatementContext) MergeScanDetail(scanDetail *util.ScanDetail) {
	// Currently TiFlash cop task does not fill scanDetail, so need to skip it if scanDetail is nil
	if scanDetail == nil {
		return
	}
	if sc.mu.execDetails.ScanDetail == nil {
		sc.mu.execDetails.ScanDetail = &util.ScanDetail{}
	}
	sc.mu.execDetails.ScanDetail.Merge(scanDetail)
}

// MergeTimeDetail merges time details into self.
func (sc *StatementContext) MergeTimeDetail(timeDetail util.TimeDetail) {
	sc.mu.execDetails.TimeDetail.ProcessTime += timeDetail.ProcessTime
	sc.mu.execDetails.TimeDetail.WaitTime += timeDetail.WaitTime
}

// MergeLockKeysExecDetails merges lock keys execution details into self.
func (sc *StatementContext) MergeLockKeysExecDetails(lockKeys *util.LockKeysDetails) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.mu.execDetails.LockKeysDetail == nil {
		sc.mu.execDetails.LockKeysDetail = lockKeys
	} else {
		sc.mu.execDetails.LockKeysDetail.Merge(lockKeys)
	}
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	var details execdetails.ExecDetails
	sc.mu.Lock()
	defer sc.mu.Unlock()
	details = sc.mu.execDetails
	details.LockKeysDuration = time.Duration(atomic.LoadInt64(&sc.LockKeysDuration))
	return details
}

// PushDownFlags converts StatementContext to tipb.SelectRequest.Flags.
func (sc *StatementContext) PushDownFlags() uint64 {
	var flags uint64
	ec := sc.ErrCtx()
	if sc.InInsertStmt {
		flags |= model.FlagInInsertStmt
	} else if sc.InUpdateStmt || sc.InDeleteStmt {
		flags |= model.FlagInUpdateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= model.FlagInSelectStmt
	}
	if sc.TypeFlags().IgnoreTruncateErr() {
		flags |= model.FlagIgnoreTruncate
	} else if sc.TypeFlags().TruncateAsWarning() {
		flags |= model.FlagTruncateAsWarning
		// TODO: remove this flag from TiKV.
		flags |= model.FlagOverflowAsWarning
	}
	if sc.TypeFlags().IgnoreZeroInDate() {
		flags |= model.FlagIgnoreZeroInDate
	}
	if ec.LevelForGroup(errctx.ErrGroupDividedByZero) != errctx.LevelError {
		flags |= model.FlagDividedByZeroAsWarning
	}
	if sc.InLoadDataStmt {
		flags |= model.FlagInLoadDataStmt
	}
	if sc.InRestrictedSQL {
		flags |= model.FlagInRestrictedSQL
	}
	return flags
}

// CopTasksDetails returns some useful information of cop-tasks during execution.
func (sc *StatementContext) CopTasksDetails() *CopTasksDetails {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := sc.mu.detailsSummary.NumCopTasks
	d := &CopTasksDetails{
		NumCopTasks:       n,
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}
	if n == 0 {
		return d
	}
	d.AvgProcessTime = sc.mu.execDetails.TimeDetail.ProcessTime / time.Duration(n)
	d.AvgWaitTime = sc.mu.execDetails.TimeDetail.WaitTime / time.Duration(n)

	d.P90ProcessTime = time.Duration((sc.mu.detailsSummary.ProcessTimePercentile.GetPercentile(0.9)))
	d.MaxProcessTime = sc.mu.detailsSummary.ProcessTimePercentile.GetMax().D
	d.MaxProcessAddress = sc.mu.detailsSummary.ProcessTimePercentile.GetMax().Addr

	d.P90WaitTime = time.Duration((sc.mu.detailsSummary.WaitTimePercentile.GetPercentile(0.9)))
	d.MaxWaitTime = sc.mu.detailsSummary.WaitTimePercentile.GetMax().D
	d.MaxWaitAddress = sc.mu.detailsSummary.WaitTimePercentile.GetMax().Addr

	for backoff, items := range sc.mu.detailsSummary.BackoffInfo {
		if items == nil {
			continue
		}
		n := items.ReqTimes
		d.MaxBackoffAddress[backoff] = items.BackoffPercentile.GetMax().Addr
		d.MaxBackoffTime[backoff] = items.BackoffPercentile.GetMax().D
		d.P90BackoffTime[backoff] = time.Duration(items.BackoffPercentile.GetPercentile(0.9))

		d.AvgBackoffTime[backoff] = items.TotBackoffTime / time.Duration(n)
		d.TotBackoffTime[backoff] = items.TotBackoffTime
		d.TotBackoffTimes[backoff] = items.TotBackoffTimes
	}
	return d
}

// InitFromPBFlagAndTz set the flag and timezone of StatementContext from a `tipb.SelectRequest.Flags` and `*time.Location`.
func (sc *StatementContext) InitFromPBFlagAndTz(flags uint64, tz *time.Location) {
	sc.InInsertStmt = (flags & model.FlagInInsertStmt) > 0
	sc.InSelectStmt = (flags & model.FlagInSelectStmt) > 0
	sc.InDeleteStmt = (flags & model.FlagInUpdateOrDeleteStmt) > 0
	levels := sc.ErrLevels()
	levels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(false,
		(flags&model.FlagDividedByZeroAsWarning) > 0,
	)
	sc.SetErrLevels(levels)
	sc.SetTimeZone(tz)
	sc.SetTypeFlags(types.DefaultStmtFlags.
		WithIgnoreTruncateErr((flags & model.FlagIgnoreTruncate) > 0).
		WithTruncateAsWarning((flags & model.FlagTruncateAsWarning) > 0).
		WithIgnoreZeroInDate((flags & model.FlagIgnoreZeroInDate) > 0).
		WithAllowNegativeToUnsigned(!sc.InInsertStmt))
}

// GetLockWaitStartTime returns the statement pessimistic lock wait start time
func (sc *StatementContext) GetLockWaitStartTime() time.Time {
	startTime := atomic.LoadInt64(&sc.lockWaitStartTime)
	if startTime == 0 {
		startTime = time.Now().UnixNano()
		atomic.StoreInt64(&sc.lockWaitStartTime, startTime)
	}
	return time.Unix(0, startTime)
}

// RecordRangeFallback records range fallback.
func (sc *StatementContext) RecordRangeFallback(rangeMaxSize int64) {
	// If range fallback happens, it means ether the query is unreasonable(for example, several long IN lists) or tidb_opt_range_max_size is too small
	// and the generated plan is probably suboptimal. In that case we don't put it into plan cache.
	if sc.UseCache {
		sc.SetSkipPlanCache(errors.NewNoStackError("in-list is too long"))
	}
	if !sc.RangeFallback {
		sc.AppendWarning(errors.NewNoStackErrorf("Memory capacity of %v bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen", rangeMaxSize))
		sc.RangeFallback = true
	}
}

// UseDynamicPartitionPrune indicates whether dynamic partition is used during the query
func (sc *StatementContext) UseDynamicPartitionPrune() bool {
	return sc.UseDynamicPruneMode
}

// DetachMemDiskTracker detaches the memory and disk tracker from the sessionTracker.
func (sc *StatementContext) DetachMemDiskTracker() {
	if sc == nil {
		return
	}
	if sc.MemTracker != nil {
		sc.MemTracker.Detach()
	}
	if sc.DiskTracker != nil {
		sc.DiskTracker.Detach()
	}
}

// SetStaleTSOProvider sets the stale TSO provider.
func (sc *StatementContext) SetStaleTSOProvider(eval func() (uint64, error)) {
	sc.StaleTSOProvider.Lock()
	defer sc.StaleTSOProvider.Unlock()
	sc.StaleTSOProvider.value = nil
	sc.StaleTSOProvider.eval = eval
}

// GetStaleTSO returns the TSO for stale-read usage which calculate from PD's last response.
func (sc *StatementContext) GetStaleTSO() (uint64, error) {
	sc.StaleTSOProvider.Lock()
	defer sc.StaleTSOProvider.Unlock()
	if sc.StaleTSOProvider.value != nil {
		return *sc.StaleTSOProvider.value, nil
	}
	if sc.StaleTSOProvider.eval == nil {
		return 0, nil
	}
	tso, err := sc.StaleTSOProvider.eval()
	if err != nil {
		return 0, err
	}
	sc.StaleTSOProvider.value = &tso
	return tso, nil
}

// AddSetVarHintRestore records the variables which are affected by SET_VAR hint. And restore them to the old value later.
func (sc *StatementContext) AddSetVarHintRestore(name, val string) {
	if sc.SetVarHintRestore == nil {
		sc.SetVarHintRestore = make(map[string]string)
	}
	sc.SetVarHintRestore[name] = val
}

// CopTasksDetails collects some useful information of cop-tasks during execution.
type CopTasksDetails struct {
	NumCopTasks int

	AvgProcessTime    time.Duration
	P90ProcessTime    time.Duration
	MaxProcessAddress string
	MaxProcessTime    time.Duration

	AvgWaitTime    time.Duration
	P90WaitTime    time.Duration
	MaxWaitAddress string
	MaxWaitTime    time.Duration

	MaxBackoffTime    map[string]time.Duration
	MaxBackoffAddress map[string]string
	AvgBackoffTime    map[string]time.Duration
	P90BackoffTime    map[string]time.Duration
	TotBackoffTime    map[string]time.Duration
	TotBackoffTimes   map[string]int
}

// ToZapFields wraps the CopTasksDetails as zap.Fileds.
func (d *CopTasksDetails) ToZapFields() (fields []zap.Field) {
	if d.NumCopTasks == 0 {
		return
	}
	fields = make([]zap.Field, 0, 10)
	fields = append(fields, zap.Int("num_cop_tasks", d.NumCopTasks))
	fields = append(fields, zap.String("process_avg_time", strconv.FormatFloat(d.AvgProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_p90_time", strconv.FormatFloat(d.P90ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_time", strconv.FormatFloat(d.MaxProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_addr", d.MaxProcessAddress))
	fields = append(fields, zap.String("wait_avg_time", strconv.FormatFloat(d.AvgWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_p90_time", strconv.FormatFloat(d.P90WaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_time", strconv.FormatFloat(d.MaxWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.MaxWaitAddress))
	return fields
}

// GetUsedStatsInfo returns the map for recording the used stats during query.
// If initIfNil is true, it will initialize it when this map is nil.
func (sc *StatementContext) GetUsedStatsInfo(initIfNil bool) map[int64]*UsedStatsInfoForTable {
	if sc.usedStatsInfo == nil && initIfNil {
		sc.usedStatsInfo = make(map[int64]*UsedStatsInfoForTable)
	}
	return sc.usedStatsInfo
}

// RecordedStatsLoadStatusCnt returns the total number of recorded column/index stats status, which is not full loaded.
func (sc *StatementContext) RecordedStatsLoadStatusCnt() (cnt int) {
	allStatus := sc.GetUsedStatsInfo(false)
	for _, status := range allStatus {
		if status == nil {
			continue
		}
		cnt += status.recordedColIdxCount()
	}
	return
}

// TypeCtxOrDefault returns the reference to the `TypeCtx` inside the statement context.
// If the statement context is nil, it'll return a newly created default type context.
// **don't** use this function if you can make sure the `sc` is not nil. We should limit the usage of this function as
// little as possible.
func (sc *StatementContext) TypeCtxOrDefault() types.Context {
	if sc != nil {
		return sc.typeCtx
	}

	return types.DefaultStmtNoWarningContext
}

func newErrCtx(tc types.Context, otherLevels errctx.LevelMap, handler contextutil.WarnHandler) errctx.Context {
	l := errctx.LevelError
	if flags := tc.Flags(); flags.IgnoreTruncateErr() {
		l = errctx.LevelIgnore
	} else if flags.TruncateAsWarning() {
		l = errctx.LevelWarn
	}

	otherLevels[errctx.ErrGroupTruncate] = l
	return errctx.NewContextWithLevels(otherLevels, handler)
}

// UsedStatsInfoForTable records stats that are used during query and their information.
type UsedStatsInfoForTable struct {
	Name                  string
	TblInfo               *model.TableInfo
	Version               uint64
	RealtimeCount         int64
	ModifyCount           int64
	ColumnStatsLoadStatus map[int64]string
	IndexStatsLoadStatus  map[int64]string
}

// FormatForExplain format the content in the format expected to be printed in the execution plan.
// case 1: if stats version is 0, print stats:pseudo.
// case 2: if stats version is not 0, and there are column/index stats that are not full loaded,
// print stats:partial, then print status of 3 column/index status at most. For the rest, only
// the count will be printed, in the format like (more: 1 onlyCmsEvicted, 2 onlyHistRemained).
func (s *UsedStatsInfoForTable) FormatForExplain() string {
	// statistics.PseudoVersion == 0
	if s.Version == 0 {
		return "stats:pseudo"
	}
	var b strings.Builder
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) == 0 {
		return ""
	}
	b.WriteString("stats:partial")
	outputNumsLeft := 3
	statusCnt := make(map[string]uint64, 1)
	var strs []string
	strs = append(strs, s.collectFromColOrIdxStatus(false, &outputNumsLeft, statusCnt)...)
	strs = append(strs, s.collectFromColOrIdxStatus(true, &outputNumsLeft, statusCnt)...)
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	if len(statusCnt) > 0 {
		b.WriteString("...(more: ")
		keys := maps.Keys(statusCnt)
		slices.Sort(keys)
		var cntStrs []string
		for _, key := range keys {
			cntStrs = append(cntStrs, strconv.FormatUint(statusCnt[key], 10)+" "+key)
		}
		b.WriteString(strings.Join(cntStrs, ", "))
		b.WriteString(")")
	}
	b.WriteString("]")
	return b.String()
}

// WriteToSlowLog format the content in the format expected to be printed to the slow log, then write to w.
// The format is table name partition name:version[realtime row count;modify count][index load status][column load status].
func (s *UsedStatsInfoForTable) WriteToSlowLog(w io.Writer) {
	ver := "pseudo"
	// statistics.PseudoVersion == 0
	if s.Version != 0 {
		ver = strconv.FormatUint(s.Version, 10)
	}
	fmt.Fprintf(w, "%s:%s[%d;%d]", s.Name, ver, s.RealtimeCount, s.ModifyCount)
	if ver == "pseudo" {
		return
	}
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) > 0 {
		fmt.Fprintf(w,
			"[%s][%s]",
			strings.Join(s.collectFromColOrIdxStatus(false, nil, nil), ","),
			strings.Join(s.collectFromColOrIdxStatus(true, nil, nil), ","),
		)
	}
}

// collectFromColOrIdxStatus prints the status of column or index stats to a slice
// of the string in the format of "col/idx name:status".
// If outputNumsLeft is not nil, this function will output outputNumsLeft column/index
// status at most, the rest will be counted in statusCnt, which is a map of status->count.
func (s *UsedStatsInfoForTable) collectFromColOrIdxStatus(
	forColumn bool,
	outputNumsLeft *int,
	statusCnt map[string]uint64,
) []string {
	var status map[int64]string
	if forColumn {
		status = s.ColumnStatsLoadStatus
	} else {
		status = s.IndexStatsLoadStatus
	}
	keys := maps.Keys(status)
	slices.Sort(keys)
	strs := make([]string, 0, len(status))
	for _, id := range keys {
		if outputNumsLeft == nil || *outputNumsLeft > 0 {
			var name string
			if s.TblInfo != nil {
				if forColumn {
					name = s.TblInfo.FindColumnNameByID(id)
				} else {
					name = s.TblInfo.FindIndexNameByID(id)
				}
			}
			if len(name) == 0 {
				name = "ID " + strconv.FormatInt(id, 10)
			}
			strs = append(strs, name+":"+status[id])
			if outputNumsLeft != nil {
				*outputNumsLeft--
			}
		} else if statusCnt != nil {
			statusCnt[status[id]] = statusCnt[status[id]] + 1
		}
	}
	return strs
}

func (s *UsedStatsInfoForTable) recordedColIdxCount() int {
	return len(s.IndexStatsLoadStatus) + len(s.ColumnStatsLoadStatus)
}

// StatsLoadResult indicates result for StatsLoad
type StatsLoadResult struct {
	Item  model.TableItemID
	Error error
}

// HasError returns whether result has error
func (r StatsLoadResult) HasError() bool {
	return r.Error != nil
}

// ErrorMsg returns StatsLoadResult err msg
func (r StatsLoadResult) ErrorMsg() string {
	if r.Error == nil {
		return ""
	}
	b := bytes.NewBufferString("tableID:")
	b.WriteString(strconv.FormatInt(r.Item.TableID, 10))
	b.WriteString(", id:")
	b.WriteString(strconv.FormatInt(r.Item.ID, 10))
	b.WriteString(", isIndex:")
	b.WriteString(strconv.FormatBool(r.Item.IsIndex))
	b.WriteString(", err:")
	b.WriteString(r.Error.Error())
	return b.String()
}
