/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Placement math for phase-2.5 shuffle. Pins down:
 * <ul>
 *   <li>bucket width heuristic under different agent counts</li>
 *   <li>determinism — same key → same bucket every time</li>
 *   <li>NULL-safe key extraction (all-null rows go to a stable bucket)</li>
 *   <li>reasonable distribution (10 buckets, 100 unique keys, no bucket &gt; 3× average)</li>
 * </ul>
 */
class ShufflePlacementTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void chooseWidth_zeroAgents_returnsOne() {
        assertThat(ShufflePlacement.chooseWidth(0, 16)).isEqualTo(1);
    }

    @Test
    void chooseWidth_agentsBelowMax_returnsAgentCount() {
        assertThat(ShufflePlacement.chooseWidth(5, 16)).isEqualTo(5);
    }

    @Test
    void chooseWidth_agentsAboveMax_capsAtMax() {
        assertThat(ShufflePlacement.chooseWidth(100, 16)).isEqualTo(16);
    }

    @Test
    void bucketFor_isDeterministic() throws Exception {
        JsonNode row = MAPPER.readTree("{\"lang\":\"en\",\"region\":\"us\"}");
        int a = ShufflePlacement.bucketFor(row, List.of("lang", "region"), 8);
        int b = ShufflePlacement.bucketFor(row, List.of("lang", "region"), 8);
        assertThat(a).isEqualTo(b);
        assertThat(a).isBetween(0, 7);
    }

    @Test
    void bucketFor_differentKeys_typicallyMapDifferent() throws Exception {
        JsonNode a = MAPPER.readTree("{\"lang\":\"en\"}");
        JsonNode b = MAPPER.readTree("{\"lang\":\"fr\"}");
        // Not a strict guarantee for 2 keys / N buckets, but with N=100 the collision
        // probability is 1/100 — this test would only flake on that specific hash coincidence.
        int ba = ShufflePlacement.bucketFor(a, List.of("lang"), 100);
        int bb = ShufflePlacement.bucketFor(b, List.of("lang"), 100);
        assertThat(ba).isNotEqualTo(bb);
    }

    @Test
    void bucketFor_nullValues_bucketStably() throws Exception {
        JsonNode allNull = MAPPER.readTree("{\"lang\":null,\"region\":null}");
        JsonNode missing = MAPPER.readTree("{}");
        // Both rows have "no lang" — should hash to the same bucket.
        assertThat(ShufflePlacement.bucketFor(allNull, List.of("lang", "region"), 8))
                .isEqualTo(ShufflePlacement.bucketFor(missing, List.of("lang", "region"), 8));
    }

    @Test
    void bucketFor_zeroBuckets_throws() {
        assertThatThrownBy(() -> ShufflePlacement.bucketFor(MAPPER.nullNode(), List.of("x"), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bucketFor_distributionIsReasonable() throws Exception {
        int buckets = 10;
        int keys = 1000;
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < keys; i++) {
            JsonNode row = MAPPER.readTree("{\"k\":\"key-" + i + "\"}");
            int b = ShufflePlacement.bucketFor(row, List.of("k"), buckets);
            counts.merge(b, 1, Integer::sum);
        }
        assertThat(counts).hasSize(buckets);   // every bucket got at least one
        int max = counts.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
        int min = counts.values().stream().mapToInt(Integer::intValue).min().orElseThrow();
        // Should be roughly uniform — reject wildly skewed distributions.
        assertThat(max).isLessThan(3 * min);
    }
}
