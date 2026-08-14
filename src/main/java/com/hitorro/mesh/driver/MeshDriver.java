/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.mesh.MeshTransport;

/**
 * Facade that stitches the driver components together: hand it a
 * {@link MeshTransport} + a {@link DistributedTableRegistry}, call
 * {@link #start()}, then submit queries via {@link #dispatcher()}.
 *
 * <p>Kept intentionally thin so a Spring Boot module can wrap this without
 * pulling framework code into the core driver package. A REST controller
 * that exposes {@code POST /queries} lives above this in the
 * {@code hitorro-mesh-driver-spring} layer (phase 1.5).</p>
 */
public final class MeshDriver implements AutoCloseable {

    private final MeshTransport transport;
    private final DistributedTableRegistry tables;
    private final LiveAgentRegistry agents;
    private final QueryDispatcher dispatcher;

    public MeshDriver(MeshTransport transport,
                      DistributedTableRegistry tables,
                      long agentExpiryMillis) {
        this.transport = transport;
        this.tables = tables;
        this.agents = new LiveAgentRegistry(transport, agentExpiryMillis);
        this.dispatcher = new QueryDispatcher(transport, tables, this.agents);
    }

    public void start() {
        agents.start();
    }

    public DistributedTableRegistry tables() { return tables; }
    public LiveAgentRegistry agents() { return agents; }
    public QueryDispatcher dispatcher() { return dispatcher; }

    @Override
    public void close() {
        agents.close();
    }
}
