/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.jsontypesystem.Type;

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
     * @param key unique partition ID within the table (e.g. {@code "shard-3"})
     * @param requiredCapabilities every agent that holds this partition must advertise all of these.
     *                             Typically {@code {"jvssql", "partition:<table>:<key>"}}.
     * @param approxRowCount optimizer hint; {@code -1} if unknown
     */
    record Partition(String key, Set<String> requiredCapabilities, long approxRowCount) {}
}
