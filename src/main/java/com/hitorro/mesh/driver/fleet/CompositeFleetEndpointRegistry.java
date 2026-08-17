/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.driver.fleet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Merges endpoints from multiple registries — typically a
 * platform-discovery registry (Kubernetes / Orion) plus a static
 * config fallback. De-dupes while preserving insertion order.
 *
 * <p>Usage:</p>
 * <pre>
 * FleetEndpointRegistry composite = new CompositeFleetEndpointRegistry(List.of(
 *         kubernetesRegistry,   // auto-discovered
 *         staticRegistry        // config fallback
 * ));
 * </pre>
 */
public final class CompositeFleetEndpointRegistry implements FleetEndpointRegistry {

    private final List<FleetEndpointRegistry> delegates;

    public CompositeFleetEndpointRegistry(List<FleetEndpointRegistry> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    @Override public String platform() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < delegates.size(); i++) {
            if (i > 0) sb.append('+');
            sb.append(delegates.get(i).platform());
        }
        return sb.toString();
    }

    @Override public List<String> endpoints() {
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        for (FleetEndpointRegistry d : delegates) {
            seen.addAll(d.endpoints());
        }
        return new ArrayList<>(seen);
    }
}
