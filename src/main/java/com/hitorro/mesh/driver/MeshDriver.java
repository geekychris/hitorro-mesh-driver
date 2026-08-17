/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.EnableS3Message;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.RegisterTableMessage;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.UnregisterTableMessage;

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
    /** Exposed so app-side helpers (inventory probes, control messages
     *  built on top of publish/subscribe) can reach the transport
     *  without re-plumbing it through Spring. */
    public MeshTransport transport() { return transport; }

    /**
     * Publish a {@link RegisterTableMessage} so every live agent installs
     * a new table at runtime — no restart. Called by the driver-app after
     * a {@code /mesh/queries/write} completes when the caller asked to
     * make the output queryable. Agent-side handling lives in
     * {@code RuntimeTableInstaller}; agents that don't have that class
     * on their classpath silently ignore the message (no runtime table
     * registry mutation).
     */
    public void publishRegisterTable(RegisterTableMessage msg) {
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(msg));
    }

    /**
     * Publish an {@link EnableS3Message} so every live agent installs a
     * MinIO / S3 protocol adapter at runtime. Called by the driver's
     * MinIO lifecycle service right after it wires its own adapter, so
     * one click on the driver's "Start MinIO" enables S3 mesh-wide.
     */
    public void publishEnableS3(EnableS3Message msg) {
        transport.publish(Subjects.agentControlEnableS3(), Codecs.encode(msg));
    }

    /**
     * Symmetric to {@link #publishRegisterTable} — tells every live agent
     * to drop a runtime-registered table from its
     * {@link com.hitorro.mesh.agent.RuntimeTableRegistry}. Combined with
     * driver-side {@code DistributedTableRegistry.unregisterBroadcast} +
     * {@code unregister} the whole table disappears from the mesh.
     */
    public void publishUnregisterTable(UnregisterTableMessage msg) {
        transport.publish(Subjects.agentControlUnregisterTable(), Codecs.encode(msg));
    }

    @Override
    public void close() {
        agents.close();
    }
}
