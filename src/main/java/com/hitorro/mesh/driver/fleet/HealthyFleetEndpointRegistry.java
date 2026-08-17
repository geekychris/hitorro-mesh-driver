/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.driver.fleet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Decorator around any {@link FleetEndpointRegistry} that filters out
 * endpoints failing repeated {@code /actuator/health} probes. Prevents
 * futile HTTP fan-out during rolling restarts, node failures, or
 * partial network partitions.
 *
 * <p>Model: every {@code checkInterval} it probes every endpoint
 * currently exposed by the delegate registry. An endpoint is dropped
 * from {@link #endpoints()} after
 * {@code failureThreshold} consecutive probe failures, and restored
 * on the first successful probe. Startup grace: newly discovered
 * endpoints are assumed healthy until the first probe completes so a
 * cold-start doesn't hide every fleet.</p>
 *
 * <p>Cheap defaults (10s interval, 3 failures to drop, 2s probe
 * timeout) fit the standard k8s readiness cadence — see the constructor
 * for full customization.</p>
 */
public final class HealthyFleetEndpointRegistry implements FleetEndpointRegistry, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HealthyFleetEndpointRegistry.class);

    private final FleetEndpointRegistry delegate;
    private final HttpClient http;
    private final Duration probeTimeout;
    private final int failureThreshold;

    /** Per-endpoint consecutive failure count. Missing key = healthy. */
    private final Map<String, Integer> failures = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public HealthyFleetEndpointRegistry(FleetEndpointRegistry delegate) {
        this(delegate, Duration.ofSeconds(10), Duration.ofSeconds(2), 3);
    }

    public HealthyFleetEndpointRegistry(FleetEndpointRegistry delegate,
                                        Duration checkInterval,
                                        Duration probeTimeout,
                                        int failureThreshold) {
        this.delegate = delegate;
        this.probeTimeout = probeTimeout;
        this.failureThreshold = failureThreshold;
        this.http = HttpClient.newBuilder().connectTimeout(probeTimeout).build();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "fleet-health-probe");
            t.setDaemon(true);
            return t;
        });
        long ms = checkInterval.toMillis();
        scheduler.scheduleAtFixedRate(this::probeAll, ms, ms, TimeUnit.MILLISECONDS);
    }

    @Override public String platform() { return delegate.platform() + "+health"; }

    @Override
    public List<String> endpoints() {
        List<String> raw = delegate.endpoints();
        if (raw.isEmpty()) return raw;
        List<String> healthy = new ArrayList<>(raw.size());
        for (String ep : raw) {
            int f = failures.getOrDefault(ep, 0);
            if (f < failureThreshold) healthy.add(ep);
        }
        return healthy;
    }

    /** Per-endpoint health detail — useful for /mesh/retrieval/federated/status. */
    public Map<String, Object> healthReport() {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String ep : delegate.endpoints()) {
            int f = failures.getOrDefault(ep, 0);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("consecutiveFailures", f);
            row.put("healthy", f < failureThreshold);
            out.put(ep, row);
        }
        return out;
    }

    private void probeAll() {
        for (String ep : delegate.endpoints()) probeOne(ep);
    }

    private void probeOne(String baseUrl) {
        try {
            HttpResponse<Void> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/actuator/health"))
                            .timeout(probeTimeout).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
            // Spring Boot returns 200 (UP) or 503 (DOWN/OUT_OF_SERVICE). Treat
            // 503 as "reachable but degraded" — still count it as healthy from
            // the driver's fan-out perspective, since fleet-retrieval reports
            // OUT_OF_SERVICE when the mesh has uncovered partitions but its
            // own KV/index serving is fine. Drop only on transport failure or
            // 5xx > 503 / 4xx.
            int sc = r.statusCode();
            boolean ok = sc == 200 || sc == 503;
            if (ok) {
                if (failures.remove(baseUrl) != null) {
                    log.info("fleet: {} back to healthy (HTTP {})", baseUrl, sc);
                }
            } else {
                bumpFailure(baseUrl, "HTTP " + sc);
            }
        } catch (Exception e) {
            bumpFailure(baseUrl, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void bumpFailure(String baseUrl, String reason) {
        int f = failures.merge(baseUrl, 1, Integer::sum);
        if (f == failureThreshold) {
            log.warn("fleet: {} dropped after {} consecutive failures ({})",
                    baseUrl, f, reason);
        } else if (f > failureThreshold && (f - failureThreshold) % 10 == 0) {
            // Periodic re-log so operators know it's still down without spam.
            log.warn("fleet: {} still down after {} failures ({})", baseUrl, f, reason);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
