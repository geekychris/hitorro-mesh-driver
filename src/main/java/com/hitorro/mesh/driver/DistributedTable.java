/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;

import java.util.List;
import java.util.Set;

/**
 * Driver-side view of a partitioned table. The driver doesn't hold data —
 * it just knows the table's shape and which partitions exist. Which agent
 * holds which partition is determined at dispatch time by asking the
 * {@link LiveAgentRegistry} for agents advertising the right capability
 * (typically {@code partition:<table>:<key>}).
 *
 * <p>A non-partitioned table is expressed as one {@link Partition} whose
 * {@code key} is the empty string.</p>
 */
public interface DistributedTable {

    String name();

    Type type();

    List<Partition> partitions();

    /**
     * Phase 6d.1 — non-null iff this table's underlying source is a stream
     * (Kafka topic, NATS JetStream subject, in-memory streaming table).
     * Signals to the planner that windowed aggregates should be planned as
     * a long-lived {@code StreamingSimplePlan} (single-agent, no cross-
     * partition combine) instead of a batch {@code TwoStagePlan} which
     * would buffer forever waiting for EOS. Default null → batch source.
     */
    default StreamConfig streamConfig() { return null; }

    /**
     * @param key unique partition ID within the table (e.g. {@code "shard-3"})
     * @param requiredCapabilities every agent that holds this partition must advertise all of these.
     *                             Typically {@code {"jvssql", "partition:<table>:<key>"}}.
     * @param approxRowCount optimizer hint; {@code -1} if unknown
     */
    record Partition(String key, Set<String> requiredCapabilities, long approxRowCount) {}
}
