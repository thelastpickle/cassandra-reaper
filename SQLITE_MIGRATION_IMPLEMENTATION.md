# SQLite Migration Implementation - Complete Guide

## Overview

This document explains the **EclipseStore to SQLite migration** implementation in Cassandra Reaper, including the logical design decisions, memory management optimizations, and critical compatibility issues discovered during testing.

---

## 1. Migration Execution Flow

###  When Does Migration Run?

The migration executes automatically **once** when Reaper starts with `storageType: memory` and `persistenceStoragePath` set:

```java
// In MemoryStorageFacade constructor
if (isPersistent && EclipseStore data exists) {
    1. Initialize SQLite schema (creates empty tables)
    2. Load EclipseStore data into memory
    3. Migrate all entities to SQLite
    4. Backup EclipseStore files to .eclipsestore.backup/
    5. Mark migration complete
}
```

### Detection Logic

```java
public static boolean migrateIfNeeded(File storageDir, Connection sqliteConnection) {
    // Check for EclipseStore channel_0 directory
    File eclipseStoreChannel = new File(storageDir, "channel_0");
    
    // Check if already migrated (backup exists)
    File backupDir = new File(storageDir, ".eclipsestore.backup");
    
    if (!eclipseStoreChannel.exists() || backupDir.exists()) {
        return false; // Nothing to migrate or already done
    }
    
    // Perform migration...
    return true;
}
```

---

## 2. Memory Management & Query Optimization

### Problem: Loading All Data Causes OutOfMemoryError

**EclipseStore loaded the entire object graph** into heap memory. With large repair histories (millions of segments), this caused OOM errors.

### Solution: Query-Based Lazy Loading

The SQLite implementation uses **prepared statements with targeted queries** instead of loading everything:

#### Example: Getting Repair Segments by Run ID

**Before (EclipseStore)**:
```java
// Load ENTIRE segment map into memory
Map<UUID, RepairSegment> allSegments = root.getRepairSegments();

// Filter in application code
List<RepairSegment> result = allSegments.values().stream()
    .filter(seg -> seg.getRepairRunId().equals(runId))
    .collect(Collectors.toList());

// Memory usage: O(total_segments)
```

**After (SQLite)**:
```java
// Query only the segments we need
PreparedStatement stmt = 
    "SELECT * FROM repair_segment WHERE repair_run_id = ?";
stmt.setBytes(1, UuidUtil.toBytes(runId));

ResultSet rs = stmt.executeQuery();
// Memory usage: O(segments_for_this_run)
```

### DAO Query Design Principles

Each DAO method follows these principles:

1. **Fetch Only What's Needed**: Use WHERE clauses to filter at database level
2. **Streaming Results**: Process ResultSet row-by-row, not load all into memory
3. **Connection Synchronization**: All PreparedStatement usage is synchronized on the connection
4. **Thread-Safe Batching**: Batch inserts use synchronized blocks

#### Example Query Decisions

| Operation | Query Strategy | Memory Impact |
|-----------|---------------|---------------|
| Get segments for run | `WHERE repair_run_id = ?` | O(segments_in_run) vs O(all_segments) |
| Get runs for cluster | `WHERE cluster_name = ?` | O(cluster_runs) vs O(all_runs) |
| Get next pending segment | `WHERE state = 'NOT_STARTED' LIMIT 1` | O(1) vs O(all_segments) |
| Count segments by state | `SELECT COUNT(*) WHERE state = ? AND run_id = ?` | O(1) vs O(segments_in_run) |

---

## 3. Critical Discovery: Guava Incompatibility Issue

### The Problem

During migration testing, we discovered a **critical incompatibility**:

1. **Old Reaper** (master branch) used Guava 30.x
2. **New Reaper** (SQLite branch) uses Guava 33.x
3. Guava's `RegularImmutableSet` internal structure **changed between versions**

When EclipseStore loads old data, it logs:

```
Legacy type mapping required for legacy type 
1000080:com.google.common.collect.RegularImmutableSet
Fields:
[Ljava.lang.Object; com.google.common.collect.RegularImmutableSet#elements  REMOVED 
[Ljava.lang.Object; com.google.common.collect.RegularImmutableSet#table  REMOVED 
```

**Result**: The `seedHosts` Set becomes corrupted (null internal arrays), causing:
- `NullPointerException` when trying to serialize to JSON
- Empty `[]` seed hosts after migration

### Our Solution

The migration code now:

1. **Detects corrupted Sets** by catching `NullPointerException` on `.size()` call
2. **Attempts reflection** to recover data from internal fields
3. **Falls back to placeholder** if recovery fails:

```java
if (recovered.isEmpty()) {
    LOG.warn("Recovered Set is empty - adding placeholder value 'MIGRATION_PLACEHOLDER'");
    LOG.warn("MANUAL ACTION REQUIRED: Update cluster seed hosts after migration");
    recovered.add("MIGRATION_PLACEHOLDER");
}
```

### Manual Migration Steps for Users

⚠️ **IMPORTANT**: After automatic migration completes, users **MUST** update cluster seed hosts:

```bash
# 1. Check current clusters
curl http://localhost:8080/cluster

# 2. Update each cluster with correct seed hosts
curl -X PUT "http://localhost:8080/cluster/{clusterName}" \
  -d "seedHost=actual-cassandra-host.example.com"
```

---

## 4. Schema Design & Data Mapping

### Parent-Child Relationships

The schema enforces referential integrity with foreign keys:

```sql
Cluster (parent)
    ↓ cluster_name FK
RepairUnit
    ↓ cluster_name FK + unit_id FK
RepairRun
    ↓ run_id FK
RepairSegment
```

### Type Mappings

| Java Type | SQLite Type | Conversion |
|-----------|-------------|------------|
| UUID | BLOB (16 bytes) | `UuidUtil.toBytes()` / `fromBytes()` |
| DateTime | INTEGER (epoch ms) | `SqliteHelper.toEpochMilli()` / `fromEpochMilli()` |
| Set\<String\> | TEXT (JSON array) | `SqliteHelper.toJson()` / `fromJsonStringCollection()` |
| Map\<String, String\> | TEXT (JSON object) | `SqliteHelper.toJson()` / `fromJson()` |
| Enum | TEXT | `.name()` / `Enum.valueOf()` |
| boolean | INTEGER (0/1) | Direct cast |

### Example: Migrating a Cluster

```java
private static void migrateClusters(Collection<Cluster> clusters, Connection conn) {
    String sql = "INSERT INTO cluster (name, partitioner, seed_hosts, properties, state, last_contact, namespace) VALUES (?, ?, ?, ?, ?, ?, ?)";
    
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        for (Cluster cluster : clusters) {
            stmt.setString(1, cluster.getName());
            stmt.setString(2, cluster.getPartitioner().orElse(null));
            stmt.setString(3, toJson(cluster.getSeedHosts())); // JSON: ["host1","host2"]
            stmt.setString(4, toJson(cluster.getProperties())); // JSON: {"jmxPort":7199,...}
            stmt.setString(5, cluster.getState().name()); // "ACTIVE"
            stmt.setLong(6, cluster.getLastContact().toEpochDay() * 86400000L);
            stmt.setString(7, null);
            stmt.executeUpdate();
        }
    }
}
```

---

## 5. Thread Safety Implementation

### Problem: SQLite PreparedStatements Are Not Thread-Safe

Unlike EclipseStore (which had implicit synchronization), SQLite connections and PreparedStatements **require explicit synchronization**.

### Solution: Connection-Level Locking

Every DAO method synchronizes on the connection:

```java
@Override
public RepairRun getRepairRun(UUID id) {
    synchronized (connection) {  // ← Critical for thread safety
        try {
            getRepairRunStmt.setBytes(1, UuidUtil.toBytes(id));
            try (ResultSet rs = getRepairRunStmt.executeQuery()) {
                if (rs.next()) {
                    return mapRowToRepairRun(rs);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to get repair run: {}", id, e);
            throw new RuntimeException(e);
        }
    }
    return null;
}
```

**Why Connection-Level**:
- Ensures only one thread uses the connection at a time
- Prevents "statement is not executing" errors
- Prevents ResultSet concurrency issues

---

## 6. Transaction Management

### Migration Uses Autocommit Mode

The migration **does not use explicit transactions** because:

1. The SQLite connection is already in `autocommit=true` mode (set by MemoryStorageFacade)
2. Each batch insert is automatically atomic
3. Explicit transaction management (`setAutoCommit(false)`) caused **database lock** errors

```java
// NO explicit transaction
migrateClusters(oldRoot.getClusters().values(), sqliteConn);
migrateRepairUnits(oldRoot.getRepairUnits().values(), sqliteConn);
// ... etc

// Each migrate method's batch is automatically atomic due to autocommit
```

### Why This Works

- **Foreign key constraints** ensure data integrity
- **Batch inserts** within each method are atomic
- **Idempotency**: Migration is only attempted once (backup check prevents re-runs)
- **Failure handling**: If migration fails, the backup is not created, so re-running Reaper will retry

---

## 7. Testing the Migration

### Prerequisites

1. **Old Reaper** with EclipseStore data in `/path/to/storage`
2. **New Reaper** (SQLite branch) JAR
3. **Configuration** with `storageType: memory` and `persistenceStoragePath: /path/to/storage`

### Test Steps

```bash
# 1. Verify EclipseStore data exists
ls /path/to/storage/channel_0

# 2. Delete any existing SQLite DB
rm -f /path/to/storage/reaper.db

# 3. Start new Reaper
java -jar cassandra-reaper-4.1.0-SNAPSHOT.jar server reaper-config.yaml

# 4. Check logs for migration messages
grep -i "migrat" reaper.log

# Expected output:
# INFO  - EclipseStore data detected in: /path/to/storage
# INFO  - Starting automatic migration to SQLite...
# INFO  - Migrating 5 clusters...
# INFO  - Migrating 12 repair units...
# INFO  - All data migrated successfully
# INFO  - EclipseStore data backed up to .eclipsestore.backup

# 5. Verify backup created
ls /path/to/storage/.eclipsestore.backup/channel_0

# 6. Verify SQLite database populated
sqlite3 /path/to/storage/reaper.db "SELECT COUNT(*) FROM cluster"

# 7. Update cluster seed hosts (if Guava issue occurred)
curl -X PUT "http://localhost:8080/cluster/{clusterName}" \
  -d "seedHost=your-cassandra-host"
```

---

## 8. Known Limitations

### 1. Guava Incompatibility

**Impact**: Cluster seed hosts may be lost during migration (replaced with placeholder)

**Workaround**: Manually update cluster seed hosts after migration

**Root Cause**: Guava `RegularImmutableSet` internal structure changed between versions, and EclipseStore removes the old fields during deserialization

### 2. No Rollback

**Impact**: Once migrated, you cannot revert to EclipseStore

**Mitigation**: The original EclipseStore data is backed up to `.eclipsestore.backup/`

**Recovery**: 
```bash
# If you need to roll back:
rm reaper.db
mv .eclipsestore.backup/channel_0 .
rmdir .eclipsestore.backup
# Use old Reaper version
```

### 3. Single-Writer Limitation

**Impact**: Only one Reaper instance can write to the SQLite database at a time

**Mitigation**: This is the same limitation as EclipseStore, so no regression

---

## 9. Production Deployment Recommendations

### Pre-Migration Checklist

- [ ] **Backup EclipseStore data**: `cp -r /path/to/storage /path/to/storage.backup`
- [ ] **Test migration** on a copy of production data first
- [ ] **Document current cluster seed hosts** for manual update after migration
- [ ] **Plan maintenance window** (Reaper will be briefly unavailable during migration)
- [ ] **Monitor disk space** (SQLite DB + backup = 2x storage space temporarily)

### Migration Window

1. **Stop old Reaper**
2. **Deploy new Reaper JAR**
3. **Update config** to use `storageType: memory` with `persistenceStoragePath`
4. **Start new Reaper** (migration runs automatically)
5. **Verify migration** in logs
6. **Update cluster seed hosts** via API
7. **Test basic operations** (list clusters, create/start repair)
8. **Delete backup** after confirming everything works: `rm -rf /path/to/storage/.eclipsestore.backup`

### Post-Migration Validation

```bash
# Check all clusters exist
curl http://localhost:8080/cluster | jq length

# Check repair runs are present
curl http://localhost:8080/repair_run | jq length

# Verify a specific repair run works
curl http://localhost:8080/repair_run/{runId}

# Create a test repair to verify write operations
curl -X POST "http://localhost:8080/repair_run" \
  -d "clusterName=test" \
  -d "keyspace=system" \
  -d "owner=admin"
```

---

## 10. Future Improvements

1. **Better Guava Handling**: Detect Guava version at runtime and use appropriate deserialization strategy
2. **Interactive Migration**: Prompt user for seed hosts if corrupted sets are detected
3. **Migration Progress UI**: Show real-time progress in web UI instead of just logs
4. **Parallel Batch Inserts**: Use multiple connections for faster migration of large datasets
5. **Schema Versioning**: Implement Flyway/Liquibase for future schema migrations

---

## Summary

The SQLite migration is **fully implemented** with:

✅ Automatic detection and execution  
✅ Memory-efficient query design  
✅ Thread-safe DAO implementation  
✅ Guava incompatibility workaround  
✅ Data backup for safety  
✅ Comprehensive logging  

⚠️ **Action Required**: Users **MUST** manually update cluster seed hosts after migration completes.

---

**Questions?** Check logs for detailed migration progress. All migration activity is logged at INFO level with prefix `EclipseStoreToSqliteMigration`.

