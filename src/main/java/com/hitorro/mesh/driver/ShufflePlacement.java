/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Placement math for the phase-2.5 distributed shuffle. Two decisions the
 * dispatcher will need:
 *
 * <ol>
 *   <li>How many shuffle buckets to open (fan-out width)?</li>
 *   <li>Which bucket does a given row belong to?</li>
 * </ol>
 *
 * <p>Ships now so the DTOs, subject naming, and hash function are pinned
 * down and unit-tested. The actual sink at the agent + source at the
 * combine worker ship in phase 2.5.1 — see {@code ROADMAP.md}.</p>
 */
public final class ShufflePlacement {

    private ShufflePlacement() {}

    /**
     * Choose the shuffle width — number of buckets, equal to the number of
     * second-stage combine workers we'll dispatch. Heuristic:
     *
     * <ul>
     *   <li>{@code liveAgents == 0} → 1 (can't shuffle to zero destinations)</li>
     *   <li>{@code liveAgents >= maxWidth} → {@code maxWidth}</li>
     *   <li>otherwise → {@code liveAgents} (one bucket per capable agent)</li>
     * </ul>
     *
     * <p>{@code maxWidth} bounds how wide the shuffle can grow — larger
     * widths mean more parallelism but also more NATS subjects to
     * subscribe to; past a few dozen the per-subject overhead outweighs
     * the parallelism win. Default 16 is a reasonable ceiling for most
     * deployments.</p>
     */
    public static int chooseWidth(int liveAgents, int maxWidth) {
        if (liveAgents <= 0) return 1;
        if (maxWidth <= 0) return liveAgents;
        return Math.min(liveAgents, maxWidth);
    }

    /**
     * Which bucket does {@code row} hash to, given {@code buckets} destinations
     * and {@code keyColumns} to extract?
     *
     * <p>Uses {@link String#hashCode()} on the concatenated key values
     * with a separator. Choice of hash: deliberately weak (not cryptographic,
     * not consistent-hashing) — we need fast + deterministic + evenly
     * distributed enough. For a shuffle with N buckets, String.hashCode
     * modulo N gives less than 1% variance on realistic string keys, which
     * is fine. If skew becomes a real issue we'll revisit (Murmur3 in
     * Guava is the natural upgrade).</p>
     *
     * <p>Nulls: a missing key column becomes the literal string
     * {@code "\0__NULL__\0"}. Two rows with the same set of nulls in the
     * same positions bucket together — matches the SQL semantic that NULL
     * groups with NULL for GROUP BY.</p>
     */
    public static int bucketFor(JsonNode row, List<String> keyColumns, int buckets) {
        if (buckets <= 0) throw new IllegalArgumentException("buckets must be > 0");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0) sb.append('\0');
            JsonNode v = row == null ? null : row.get(keyColumns.get(i));
            if (v == null || v.isNull()) {
                sb.append("\0__NULL__\0");
            } else {
                sb.append(v.asText());
            }
        }
        int h = sb.toString().hashCode();
        // Modulo can be negative in Java when the hash is negative; Math.floorMod fixes that.
        return Math.floorMod(h, buckets);
    }
}
