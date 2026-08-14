/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Driver-side registry of tables that can be queried across the mesh.
 *
 * <p>Two kinds of tables:</p>
 * <ul>
 *   <li><b>Distributed</b> — partitioned across agents. Every partition lives
 *       on one (or a few) capability-matched agent. Scans are dispatched to
 *       the right agent per partition.</li>
 *   <li><b>Broadcast</b> — small dimension tables replicated to <i>every</i>
 *       jvssql-capable agent. Referenced from JOINs in the driver-facing SQL;
 *       joins execute locally at each agent. Phase 4a.</li>
 * </ul>
 *
 * <p>The driver-side registration is purely metadata — the actual broadcast
 * data lives at each agent (agent-app loads it from its own NDJSON config).
 * The registry knows the name so the {@link QueryPlanner} can allow JOINs to
 * broadcast tables and reject JOINs to unknown ones.</p>
 */
public final class DistributedTableRegistry {

    // Synchronized because phase 7p added runtime add/remove via REST,
    // which can race with query dispatch (dispatchAndCollect reads tables
    // during a live query). All mutators + accessors gate on `this`.
    private final Map<String, DistributedTable> tables = new LinkedHashMap<>();
    private final Set<String> broadcastNames = new LinkedHashSet<>();

    public synchronized void register(DistributedTable t) {
        tables.put(t.name(), t);
    }

    public synchronized DistributedTable get(String name) {
        return tables.get(name);
    }

    public synchronized Collection<DistributedTable> all() {
        // Return a snapshot to keep callers safe outside the lock.
        return new java.util.ArrayList<>(tables.values());
    }

    /**
     * Phase 7p — remove a distributed table. In-flight queries against
     * this table continue with the metadata they already resolved; new
     * queries fail with the standard "no distributed table registered"
     * planner error. Returns true if the name existed.
     */
    public synchronized boolean remove(String name) {
        return tables.remove(name) != null;
    }

    /**
     * Register that a broadcast table with this name is available at every
     * jvssql-capable agent. Every agent must pre-load the table via its
     * {@link com.hitorro.mesh.agent.AgentConfig#broadcastTables()} — if the
     * table is missing on some agent, tasks touching that agent will fail
     * with a clear jvssql error.
     */
    public synchronized void registerBroadcast(String name) {
        broadcastNames.add(name);
    }

    /**
     * Phase 7p — deregister a broadcast name. Doesn't affect data on
     * agents (which continue to hold their pre-loaded tables); just tells
     * the driver's planner to reject new JOINs against this name.
     */
    public synchronized boolean unregisterBroadcast(String name) {
        return broadcastNames.remove(name);
    }

    public synchronized Set<String> broadcastNames() {
        return new LinkedHashSet<>(broadcastNames);
    }

    public synchronized boolean isBroadcast(String name) {
        return broadcastNames.contains(name);
    }

    /**
     * Phase 6d.1 — names of registered tables whose underlying source is a
     * stream (i.e. {@code table.streamConfig() != null}). Used by the
     * planner to route windowed aggregates over these sources to the
     * long-lived streaming plan instead of the batch two-stage plan.
     */
    public synchronized Set<String> streamingTableNames() {
        Set<String> out = new LinkedHashSet<>();
        for (DistributedTable t : tables.values()) {
            if (t.streamConfig() != null) out.add(t.name());
        }
        return out;
    }
}
