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

    private final Map<String, DistributedTable> tables = new LinkedHashMap<>();
    private final Set<String> broadcastNames = new LinkedHashSet<>();

    public void register(DistributedTable t) {
        tables.put(t.name(), t);
    }

    public DistributedTable get(String name) {
        return tables.get(name);
    }

    public Collection<DistributedTable> all() {
        return tables.values();
    }

    /**
     * Register that a broadcast table with this name is available at every
     * jvssql-capable agent. Every agent must pre-load the table via its
     * {@link com.hitorro.mesh.agent.AgentConfig#broadcastTables()} — if the
     * table is missing on some agent, tasks touching that agent will fail
     * with a clear jvssql error.
     */
    public void registerBroadcast(String name) {
        broadcastNames.add(name);
    }

    public Set<String> broadcastNames() {
        return broadcastNames;
    }

    public boolean isBroadcast(String name) {
        return broadcastNames.contains(name);
    }
}
