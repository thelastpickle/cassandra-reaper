# SQLite Migration Implementation Summary

**Date**: October 28, 2025  
**Status**: In Progress - Phase 1 Complete  
**Goal**: Replace EclipseStore with SQLite to reduce memory footprint by 70-90%

## ✅ Phase 1: Completed (Foundation)

### 1. SQLite Dependency & Infrastructure
- **Added** `org.xerial:sqlite-jdbc:3.47.1.0` to `pom.xml`
- **Created** SQL schema at `src/server/src/main/resources/db/sqlite/001_init_schema.sql`
  - Tables: cluster, repair_unit, repair_schedule, repair_run, repair_segment, diag_event_subscription, schema_version
  - Indexes for performance: state, run_id, cluster_name, end_time
  - Foreign keys with CASCADE for data integrity

### 2. Schema Management
- **Implemented** `SqliteMigrationManager.java`
  - Automatic schema initialization
  - Version tracking with `schema_version` table
  - SQL script loading from resources
  - Fixed: Comment line filtering (removed `--` prefixed lines before execution)
  - Fixed: Statement splitting by semicolon (handles multiline SQL)
  
### 3. Utility Classes
- **Implemented** `UuidUtil.java`
  - UUID ↔ byte array conversion for BLOB storage
  - Methods: `toBytes(UUID)`, `fromBytes(byte[])`

- **Implemented** `SqliteHelper.java`
  - JSON serialization/deserialization for complex objects
  - DateTime ↔ epoch milliseconds conversion
  - Methods:
    - `toJson(Object)`, `fromJson(String, Class<T>)`
    - `toEpochMilli(DateTime)`, `fromEpochMilli(Long)`
    - `fromJsonStringMap(String)` for Map<String, String>

### 4. EclipseStore Migration
- **Implemented** `EclipseStoreToSqliteMigration.java`
  - Automatic detection of existing EclipseStore data (checks for `channel_0` file)
  - Loads all entities from EclipseStore
  - Migrates to SQLite in a transaction
  - Backs up EclipseStore files to `.eclipsestore.backup/`
  - Handles:
    - Clusters (with ClusterProperties as JSON)
    - Repair Units
    - Repair Schedules
    - Repair Runs
    - Repair Segments
    - Diagnostic Event Subscriptions

### 5. MemoryStorageFacade Refactoring
- **Fixed** initialization order issue (DAOs now initialized in constructor after SQLite connection)
- **Added** SQLite connection management
  - In-memory mode: `jdbc:sqlite::memory:` (when persistenceStoragePath is empty)
  - Persistent mode: `jdbc:sqlite:{persistenceStoragePath}/reaper.db`
  - Connection properly closed in `stop()` method
- **Updated** `isStorageConnected()` to check SQLite connection status
- **Maintained** backward compatibility with `persistenceStoragePath` as directory path

### 6. DAO Implementations

#### ✅ MemoryClusterDao (Complete)
- Uses PreparedStatements for all operations
- SQL-based CRUD: insert, get, update, delete
- Proper JSON serialization for ClusterProperties
- Cascade delete for related schedules, runs, and subscriptions

#### ✅ MemoryRepairUnitDao (Complete)
- SQL-based operations with PreparedStatements
- Composite key queries (cluster + keyspace + column families + flags)
- JSON arrays for sets (nodes, datacenters, blacklisted tables)

#### ✅ MemoryRepairSegmentDao (Complete - needs facade delegation)
- Full SQL implementation with PreparedStatements
- Efficient queries with indexes
- Methods:
  - `addRepairSegments(Collection, UUID)` - batch insert
  - `deleteRepairSegmentsForRun(UUID)` - cascade delete
  - `updateRepairSegment()` - lightweight UPDATE
  - `getRepairSegmentsForRun(UUID)` - by run_id
  - `getNextFreeSegments(UUID)` - NOT_STARTED segments without locked nodes
  - `getSegmentsWithState(UUID, State)` - filtered by state
  - `getSegmentAmountForRepairRun*` - count queries
- Proper Segment/RingRange reconstruction from database

### 7. Bug Fixes
- **Fixed** SQL file semicolon in comment breaking parser
- **Fixed** Statement splitting to handle multiline SQL with comment filtering
- **Fixed** Duplicate schema_version INSERT (removed from SQL file)
- **Fixed** DAO initialization order (moved to constructor)
- **Fixed** Import removal by spotless (re-added required imports)

## ⚠️ Phase 2: In Progress (DAO Migration)

### Current Issue: Facade Delegation
**Problem**: Tests fail because deprecated methods in `MemoryStorageFacade` (like `addRepairSegment()`) still use the old `MemoryStorageRoot` instead of delegating to new SQL-backed DAOs.

**Impact**: MemoryRepairSegmentDao works but tests use facade methods that don't persist to SQLite.

**Solution Needed**: Update all deprecated methods in `MemoryStorageFacade` to delegate to corresponding DAOs:
```java
@Deprecated
public RepairSegment addRepairSegment(RepairSegment segment) {
  // OLD: memoryStorageRoot.addRepairSegment(segment);
  // NEW: Delegate to DAO
  try {
    memRepairSegment.insertSegment(segment); // Need to expose or use existing methods
    return segment;
  } catch (SQLException e) {
    throw new RuntimeException(e);
  }
}
```

### Remaining DAOs to Implement

#### ✅ MemoryRepairRunDao (Complete)
**Status**: Fully refactored to use SQLite  
**Complexity**: Medium  
**Key Methods Implemented**:
- `addRepairRun(RepairRun.Builder, Collection<RepairSegment.Builder>)` - INSERT with PreparedStatement
- `updateRepairRun(RepairRun)` - UPDATE with PreparedStatement
- `getRepairRun(UUID id)` - SELECT by ID
- `getRepairRunsForCluster(String cluster, Optional<Integer> limit)` - SELECT with LIMIT
- `getRepairRunsForClusterPrioritiseRunning(String, Optional<Integer>)` - SELECT with ORDER BY state
- `getRepairRunsForUnit(UUID repairUnitId)` - SELECT by unit
- `getRepairRunsWithState(RunState state)` - SELECT by state
- `deleteRepairRun(UUID id)` - DELETE with cascade to segments
- `getRepairRunIdsForCluster(String, Optional<Integer>)` - SELECT IDs only

**Schema Used**:
```sql
CREATE TABLE repair_run (
  id BLOB PRIMARY KEY,
  cluster_name TEXT NOT NULL,
  repair_unit_id BLOB NOT NULL,
  cause TEXT,
  owner TEXT,
  state TEXT NOT NULL,
  creation_time INTEGER,
  start_time INTEGER,
  end_time INTEGER,
  pause_time INTEGER,
  intensity REAL,
  last_event TEXT,
  segment_count INTEGER,
  repair_parallelism TEXT,
  tables TEXT, -- JSON array (Set<String>)
  adaptive_schedule INTEGER NOT NULL,
  FOREIGN KEY (repair_unit_id) REFERENCES repair_unit(id)
);
CREATE INDEX idx_repair_run_cluster ON repair_run(cluster_name);
CREATE INDEX idx_repair_run_state ON repair_run(state);
CREATE INDEX idx_repair_run_end_time ON repair_run(end_time);
CREATE INDEX idx_repair_run_unit ON repair_run(repair_unit_id);
```

**Features**:
- INSERT OR REPLACE for upsert semantics
- Proper RepairParallelism enum serialization (imported from `org.apache.cassandra.repair`)
- JSON serialization for tables (Set<String>)
- DateTime ↔ epoch milliseconds conversion
- Complete ResultSet → RepairRun mapping in `mapRowToRepairRun()`

#### 🔄 MemoryRepairScheduleDao (High Priority)
**Status**: Uses old MemoryStorageRoot  
**Complexity**: Medium  
**Dependencies**: repair_unit_id (foreign key)  
**Key Methods**:
- `addRepairSchedule(RepairSchedule)`
- `getRepairSchedule(UUID)`
- `getRepairSchedulesForCluster(String cluster)`
- `getRepairSchedulesForKeyspace(String cluster, String keyspace)`
- `getAllRepairSchedules()`
- `deleteRepairSchedule(UUID)`
- `updateRepairSchedule(RepairSchedule)`

#### 🔄 MemoryEventsDao (Medium Priority)
**Status**: Uses old MemoryStorageRoot  
**Complexity**: Low  
**Key Methods**:
- `addEventSubscription(DiagEventSubscription)`
- `getEventSubscription(UUID)`
- `getEventSubscriptions(String cluster)`
- `getEventSubscriptions()`
- `deleteEventSubscription(UUID)`

#### ⚠️ MemoryMetricsDao (Low Priority - In-Memory Only)
**Status**: Uses in-memory ConcurrentHashMap  
**Decision**: Keep as-is (volatile, not persisted)  
**Rationale**: User explicitly stated metrics don't need persistence

#### ⚠️ MemorySnapshotDao (Low Priority - In-Memory Only)
**Status**: Uses in-memory ConcurrentHashMap  
**Decision**: Keep as-is (volatile, not persisted)  
**Rationale**: Snapshots are temporary/ephemeral data

## 🎯 Phase 3: Cleanup & Testing (Not Started)

### Tasks
1. **Remove deprecated MemoryStorageRoot entirely**
   - Delete `MemoryStorageRoot.java`
   - Remove all references from `MemoryStorageFacade`
   - Remove EclipseStore dependency

2. **Update Tests**
   - Ensure all tests use proper DAO methods
   - Add SQLite-specific test cases
   - Test migration scenarios

3. **Performance Testing**
   - Measure memory footprint before/after
   - Benchmark query performance
   - Test with realistic data volumes

4. **Documentation**
   - Update CHANGELOG.md
   - Document migration process
   - Update configuration docs

## 📊 Expected Results

### Memory Footprint
- **Before**: Full heap storage of all entities (ConcurrentHashMaps)
- **After**: Only SQLite page cache (~10-30% of original)
- **Reduction**: 70-90% for long-running instances with large repair history

### Performance
- **Queries**: Faster with proper indexes (state, run_id, cluster)
- **Writes**: Comparable (PreparedStatements are efficient)
- **Startup**: Faster (lazy loading, no full dataset load)

### User Experience
- **Config**: No changes required
- **Migration**: Automatic on first startup
- **Rollback**: EclipseStore files backed up

## 🔧 Implementation Patterns

### Pattern 1: DAO with PreparedStatements
```java
public class MemoryXxxDao implements IXxxDao {
  private final Connection connection;
  private final PreparedStatement insertStmt;
  private final PreparedStatement getByIdStmt;
  
  public MemoryXxxDao(MemoryStorageFacade facade) {
    this.connection = facade.getSqliteConnection();
    try {
      this.insertStmt = connection.prepareStatement("INSERT INTO xxx ...");
      this.getByIdStmt = connection.prepareStatement("SELECT * FROM xxx WHERE id = ?");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to prepare statements", e);
    }
  }
  
  @Override
  public Xxx getXxx(UUID id) {
    try {
      getByIdStmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = getByIdStmt.executeQuery()) {
        if (rs.next()) {
          return mapRowToXxx(rs);
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get xxx {}", id, e);
      throw new RuntimeException(e);
    }
    return null;
  }
  
  private Xxx mapRowToXxx(ResultSet rs) throws SQLException {
    // Map ResultSet columns to domain object
    return Xxx.builder()
      .withId(UuidUtil.fromBytes(rs.getBytes("id")))
      .withName(rs.getString("name"))
      .withTimestamp(SqliteHelper.fromEpochMilli(rs.getLong("timestamp")))
      .build();
  }
}
```

### Pattern 2: JSON for Complex Objects
```java
// Storing
stmt.setString(1, SqliteHelper.toJson(setOfStrings));
stmt.setString(2, SqliteHelper.toJson(mapOfStrings));

// Loading
Set<String> set = SqliteHelper.fromJsonStringCollection(rs.getString("set_column"), HashSet.class);
Map<String, String> map = SqliteHelper.fromJsonStringMap(rs.getString("map_column"));
```

### Pattern 3: UUID as BLOB
```java
// Storing
stmt.setBytes(1, UuidUtil.toBytes(uuid));

// Loading
UUID uuid = UuidUtil.fromBytes(rs.getBytes("id"));
```

### Pattern 4: DateTime as Long
```java
// Storing
stmt.setLong(1, SqliteHelper.toEpochMilli(dateTime));

// Loading
DateTime dt = SqliteHelper.fromEpochMilli(rs.getLong("timestamp"));
```

## 🚨 Known Issues

### Issue 1: Spotless Removes Imports
**Problem**: Maven spotless:apply removes necessary imports  
**Workaround**: Run `mvn compile` without spotless, or re-add imports after spotless  
**Affected Files**: SqliteHelper.java, MemoryRepairSegmentDao.java

### Issue 2: Test Failures
**Problem**: Tests use deprecated facade methods that don't delegate to DAOs  
**Status**: Needs facade delegation updates  
**Affected Tests**: MemoryRepairSegmentDaoTest (3 failures)

## 📝 Next Steps (Immediate)

1. ✅ **Implement MemoryRepairRunDao** - COMPLETE
2. **Update MemoryStorageFacade deprecated methods** to delegate to new DAOs (CRITICAL)
3. **Fix MemoryRepairSegmentDaoTest** by updating facade delegation
4. **Implement MemoryRepairScheduleDao**
5. **Implement MemoryEventsDao**
6. **Remove MemoryStorageRoot and EclipseStore dependency**
7. **Full test suite pass**
8. **Performance benchmarking**

## 🎉 Current Status (October 28, 2025)

### ✅ Phase 1: Foundation - COMPLETE
- SQLite JDBC dependency added
- Schema created with proper indexes and foreign keys
- Utility classes implemented (UuidUtil, SqliteHelper, SqliteMigrationManager)
- EclipseStore migration utility complete
- MemoryStorageFacade refactored for SQLite

### ✅ Phase 2a: Core DAOs - COMPLETE (4/4)
- ✅ MemoryClusterDao - Full SQL implementation
- ✅ MemoryRepairUnitDao - Full SQL implementation
- ✅ MemoryRepairSegmentDao - Full SQL implementation
- ✅ MemoryRepairRunDao - Full SQL implementation

### 🔄 Phase 2b: Remaining DAOs - IN PROGRESS
- ⏳ MemoryRepairScheduleDao - Pending
- ⏳ MemoryEventsDao - Pending

### 🚧 Critical Blocker
**Issue**: Deprecated methods in MemoryStorageFacade (e.g., `addRepairSegment()`, `addRepairRun()`) still reference the old `MemoryStorageRoot` instead of delegating to new SQL-backed DAOs.

**Impact**: Tests fail because they call these deprecated facade methods, which don't persist to SQLite.

**Solution**: Update all deprecated methods to delegate to the new DAOs. For example:
```java
@Deprecated
public RepairSegment addRepairSegment(RepairSegment segment) {
  // OLD: memoryStorageRoot.addRepairSegment(segment);
  // NEW: Use DAO method or create a wrapper
  try {
    // Need to expose a method in MemoryRepairSegmentDao or handle differently
    // This is the main blocker for tests
  } catch (Exception e) {
    throw new RuntimeException(e);
  }
}
```

### 📊 Progress Summary
- **Files Created**: 5 (UuidUtil, SqliteHelper, SqliteMigrationManager, EclipseStoreToSqliteMigration, schema SQL)
- **Files Refactored**: 5 (MemoryStorageFacade, MemoryClusterDao, MemoryRepairUnitDao, MemoryRepairSegmentDao, MemoryRepairRunDao)
- **Compilation**: ✅ SUCCESS (mvn compile passes)
- **Tests**: ❌ BLOCKED (facade delegation needs fixing)

### 🎯 Estimated Remaining Work
- **High Priority** (needed for tests to pass):
  - Fix MemoryStorageFacade deprecated method delegation (2-4 hours)
  - Implement MemoryRepairScheduleDao (2-3 hours)
  - Implement MemoryEventsDao (1-2 hours)
  
- **Medium Priority** (cleanup):
  - Remove MemoryStorageRoot (30 minutes)
  - Full test suite pass (depends on above)
  
- **Low Priority** (future):
  - Performance benchmarking
  - Documentation updates

### 💡 Key Learnings
1. **RepairParallelism Import**: Must be imported from `org.apache.cassandra.repair.RepairParallelism`, not core
2. **JSON Collections**: `fromJsonStringCollection()` method handles Set/List deserialization gracefully
3. **LocalDate Conversion**: Needed separate methods for LocalDate ↔ epoch milliseconds
4. **INSERT OR REPLACE**: SQLite's upsert is cleaner than DELETE+INSERT
5. **Spotless**: Sometimes removes imports; use `-Dspotless.check.skip=true` during development

## 📚 Files Modified

### New Files Created
- `src/server/src/main/java/io/cassandrareaper/storage/sqlite/UuidUtil.java`
- `src/server/src/main/java/io/cassandrareaper/storage/sqlite/SqliteHelper.java`
- `src/server/src/main/java/io/cassandrareaper/storage/sqlite/SqliteMigrationManager.java`
- `src/server/src/main/java/io/cassandrareaper/storage/sqlite/EclipseStoreToSqliteMigration.java`
- `src/server/src/main/resources/db/sqlite/001_init_schema.sql`

### Files Modified
- `src/server/pom.xml` (added sqlite-jdbc dependency)
- `src/server/src/main/java/io/cassandrareaper/storage/MemoryStorageFacade.java` (SQLite integration)
- `src/server/src/main/java/io/cassandrareaper/storage/cluster/MemoryClusterDao.java` (refactored ✅)
- `src/server/src/main/java/io/cassandrareaper/storage/repairunit/MemoryRepairUnitDao.java` (refactored ✅)
- `src/server/src/main/java/io/cassandrareaper/storage/repairsegment/MemoryRepairSegmentDao.java` (refactored ✅)
- `src/server/src/main/java/io/cassandrareaper/storage/repairrun/MemoryRepairRunDao.java` (refactored ✅)

### Files To Be Modified
- `src/server/src/main/java/io/cassandrareaper/storage/repairschedule/MemoryRepairScheduleDao.java` (pending)
- `src/server/src/main/java/io/cassandrareaper/storage/events/MemoryEventsDao.java` (pending)

### Files To Be Deleted (Phase 3)
- `src/server/src/main/java/io/cassandrareaper/storage/memory/MemoryStorageRoot.java`

## 🎓 Lessons Learned

1. **SQL Schema Design**: Proper indexes are critical for query performance
2. **Foreign Keys**: CASCADE deletes simplify cleanup logic
3. **PreparedStatements**: Reuse across method calls for efficiency
4. **JSON Storage**: Flexible for complex structures (Sets, Maps, custom objects)
5. **UUID as BLOB**: More efficient than TEXT (16 bytes vs 36 characters)
6. **DateTime as LONG**: Simple, sortable, no timezone issues
7. **Comment Handling**: SQL parsers need careful handling of comment lines
8. **Statement Splitting**: Multiline SQL requires sophisticated parsing
9. **Migration Safety**: Always backup original data before migration
10. **DAO Initialization**: Order matters - connection before DAOs

## 🔗 References

- SQLite JDBC Driver: https://github.com/xerial/sqlite-jdbc
- SQLite Documentation: https://www.sqlite.org/docs.html
- EclipseStore: https://eclipsestore.io/
- Joda-Time: https://www.joda.org/joda-time/

