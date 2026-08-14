/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.mesh.AgentDescriptor;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.HeartbeatMessage;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.Subjects;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * "Who's alive on the mesh?" — the driver subscribes to every agent
 * heartbeat and keeps a fresh view.
 *
 * <p>An agent is considered alive if its last heartbeat is within
 * {@link #expiryMillis} of now. Reads are cheap (ConcurrentHashMap lookup);
 * expiry is checked on read, not swept periodically. Simple and correct.</p>
 */
public final class LiveAgentRegistry implements AutoCloseable {

    private final MeshTransport transport;
    private final long expiryMillis;
    private final Map<String, Entry> agents = new ConcurrentHashMap<>();
    private MeshTransport.Subscription sub;

    public LiveAgentRegistry(MeshTransport transport, long expiryMillis) {
        this.transport = transport;
        this.expiryMillis = expiryMillis;
    }

    public void start() {
        sub = transport.subscribe(Subjects.allHeartbeats(), bytes -> {
            HeartbeatMessage hb = Codecs.decode(bytes, HeartbeatMessage.class);
            agents.put(hb.agent().agentId(),
                    new Entry(hb.agent(), hb.timestampMillis(), hb.activeTaskCount()));
        });
    }

    /** Live agents that advertise all the given capabilities. Cheapest possible match. */
    public List<AgentDescriptor> agentsWith(Iterable<String> requiredCapabilities) {
        long cutoff = System.currentTimeMillis() - expiryMillis;
        List<AgentDescriptor> out = new ArrayList<>();
        for (Entry e : agents.values()) {
            if (e.lastSeen < cutoff) continue;
            if (e.agent.hasAll(requiredCapabilities)) out.add(e.agent);
        }
        return out;
    }

    /** Pick the least-loaded live agent matching the requirements, or empty if none. */
    public Optional<AgentDescriptor> pick(Iterable<String> requiredCapabilities) {
        long cutoff = System.currentTimeMillis() - expiryMillis;
        AgentDescriptor best = null;
        long bestLoad = Long.MAX_VALUE;
        for (Entry e : agents.values()) {
            if (e.lastSeen < cutoff) continue;
            if (!e.agent.hasAll(requiredCapabilities)) continue;
            if (e.activeTasks < bestLoad) {
                bestLoad = e.activeTasks;
                best = e.agent;
            }
        }
        return Optional.ofNullable(best);
    }

    public int liveCount() {
        long cutoff = System.currentTimeMillis() - expiryMillis;
        int n = 0;
        for (Entry e : agents.values()) if (e.lastSeen >= cutoff) n++;
        return n;
    }

    @Override
    public void close() {
        if (sub != null) sub.close();
    }

    private record Entry(AgentDescriptor agent, long lastSeen, long activeTasks) {}
}
