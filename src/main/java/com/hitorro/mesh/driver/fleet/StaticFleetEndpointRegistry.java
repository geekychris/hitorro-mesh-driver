/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.driver.fleet;

import java.util.Collections;
import java.util.List;

/**
 * Static {@link FleetEndpointRegistry} — endpoints come from a
 * caller-supplied list (typically {@code hitorro.fleet.endpoints} in
 * driver config).
 *
 * <p>Works on every platform, no external dependencies. Also serves as
 * a fallback when a platform-specific registry (Kubernetes, Orion)
 * discovers zero instances — merge both via
 * {@link CompositeFleetEndpointRegistry} when you want static + auto
 * discovery together.</p>
 */
public final class StaticFleetEndpointRegistry implements FleetEndpointRegistry {

    private final List<String> endpoints;

    public StaticFleetEndpointRegistry(List<String> endpoints) {
        this.endpoints = endpoints == null ? List.of() : List.copyOf(endpoints);
    }

    @Override public String platform() { return "static"; }

    @Override public List<String> endpoints() {
        return Collections.unmodifiableList(endpoints);
    }
}
