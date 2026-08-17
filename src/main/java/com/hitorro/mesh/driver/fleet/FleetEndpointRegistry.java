/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.driver.fleet;

import java.util.List;

/**
 * Discovers the set of {@code hitorro-fleet-retrieval} instances the
 * driver can federate queries across. Populated per platform:
 *
 * <ul>
 *   <li><b>Static</b> — reads {@code hitorro.fleet.endpoints} from
 *       driver config. Works on every platform; used for local dev
 *       and as a fallback when platform discovery finds nothing.</li>
 *   <li><b>Kubernetes</b> — watches Services with label
 *       {@code app=hitorro-fleet-retrieval} in the driver's namespace
 *       via {@code io.fabric8:kubernetes-client}.</li>
 *   <li><b>Orion</b> — queries the Orion cluster manager for services
 *       with the fleet-retrieval tag.</li>
 * </ul>
 *
 * <p>Endpoints are HTTP base URLs like
 * {@code http://fleet-retrieval-0.fleet-retrieval:8087}. The driver
 * wraps each in a {@code RemoteSearchProvider} and hands the composite
 * set to {@code CompositeSearchProvider} for parallel search +
 * merger-driven result fusion.</p>
 *
 * <p>Implementations should be cheap to poll ({@code endpoints()} may
 * be called on every federated query) — cache the platform-discovery
 * result with a short TTL if the platform's watch API isn't cheap.</p>
 */
public interface FleetEndpointRegistry {

    /** Human-readable name of the discovery source ("static", "kubernetes", "orion"). */
    String platform();

    /**
     * Current fleet-retrieval endpoints in no particular order.
     * Each entry is a base URL — {@code http://host:port} — the
     * {@code /api/retrieval/…} paths are appended by the client.
     */
    List<String> endpoints();
}
