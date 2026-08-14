/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.JvsSqlEngine;
import com.hitorro.jvssql.PreparedQuery;
import com.hitorro.mesh.AgentDescriptor;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.CombineSpec;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.ResultMessage;
import com.hitorro.mesh.ShuffleSpec;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.TaskDescriptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Splits a query into stages, dispatches per-partition tasks, merges result
 * streams.
 *
 * <h3>Three execution paths</h3>
 * <ul>
 *   <li><b>Simple plan</b> — projection/filter query, no GROUP BY. Each agent's
 *       output IS the final result; driver streams rows as they arrive.</li>
 *   <li><b>Two-stage combiner-at-driver</b> — GROUP BY with COUNT/SUM/MIN/MAX,
 *       shuffle-width == 0. Phase-2 behavior: partial runs per partition,
 *       combine runs at the driver over the union of partials.</li>
 *   <li><b>Two-stage distributed combiner</b> — same query shape, shuffle-width &gt; 0.
 *       Phase 2.5.1: N combine workers subscribe to N shuffle bucket subjects; each
 *       stage-0 partition hashes its output rows into those buckets; combine workers
 *       run the combine SQL over their bucket's rows and publish combined output
 *       back to the standard result subject.</li>
 * </ul>
 *
 * <p>Shuffle width is controlled by
 * {@link #withShuffleWidth(int)} — defaults to 0 (combiner-at-driver, exactly
 * matches phase 2 behavior). Set to N&gt;0 to enable distributed combine; the
 * dispatcher clamps N to the number of live jvssql-capable agents.</p>
 *
 * <h3>Design decision — why an explicit shuffle-width knob</h3>
 *
 * <p>Both paths produce identical results for associative+commutative
 * aggregates. Combiner-at-driver has zero coordination overhead and lower
 * latency for small group cardinality (&lt; ~100k groups) because it skips
 * the shuffle hop and combine-worker orchestration. Distributed combiner
 * unbounds the group cardinality but pays 2× NATS hops per row plus
 * combine-worker startup latency (~50-100ms).</p>
 *
 * <p>Rather than auto-decide, we expose the knob so deployers can set it
 * per-cluster based on their workload. Default 0 is the "safe" choice —
 * matches phase 2 behavior with zero risk of shuffle bugs affecting
 * production. Bump to non-zero once you've verified for your query shapes.</p>
 */
public final class QueryDispatcher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Milliseconds to wait for stage-1 subscriptions to establish before dispatching stage-0. */
    private static final long SHUFFLE_READY_GRACE_MS = 200;

    private final MeshTransport transport;
    private final DistributedTableRegistry tables;
    private final LiveAgentRegistry agents;
    private volatile int shuffleWidth = 0;
    private volatile int shuffleWidthCap = 16;

    public QueryDispatcher(MeshTransport transport,
                           DistributedTableRegistry tables,
                           LiveAgentRegistry agents) {
        this.transport = transport;
        this.tables = tables;
        this.agents = agents;
    }

    /**
     * @param width 0 = combiner-at-driver (phase 2), N&gt;0 = distributed combiner
     *              with up to N combine workers. Clamped to live-agent count at query time.
     */
    public QueryDispatcher withShuffleWidth(int width) {
        this.shuffleWidth = Math.max(0, width);
        return this;
    }

    public QueryDispatcher withShuffleWidthCap(int cap) {
        this.shuffleWidthCap = Math.max(1, cap);
        return this;
    }

    public int shuffleWidth() { return shuffleWidth; }

    /** Run a distributed query. */
    public QueryHandle submit(String sql) {
        // Phase 5a/5b: strip LIMIT and ORDER BY before planning — both are
        // driver-side concerns. Applied as post-processing so every plan
        // shape gets them for free (Simple, TwoStage, ShuffleJoin).
        java.util.OptionalLong limitOpt = QueryPlanner.parseLimit(sql);
        String withoutLimit = QueryPlanner.stripLimit(sql);
        List<QueryPlanner.OrderKey> orderBy = QueryPlanner.parseOrderBy(withoutLimit);
        String plannerSql = QueryPlanner.stripOrderBy(withoutLimit);

        // Both the broadcast set and the distributed set come from the registry —
        // the planner uses them to decide broadcast-JOIN (phase 4a), shuffle-JOIN
        // (phase 4b), or simple/aggregate flavors (phases 1-3).
        java.util.Set<String> distributedNames = tables.all().stream()
                .map(DistributedTable::name).collect(java.util.stream.Collectors.toSet());
        QueryPlanner.Plan plan = QueryPlanner.plan(plannerSql,
                tables.broadcastNames(), distributedNames, tables.streamingTableNames());

        QueryHandle base;
        if (plan instanceof QueryPlanner.StreamingWindowedPlan swp) {
            DistributedTable table = tables.get(swp.tableName());
            if (table == null) {
                throw new IllegalArgumentException("streaming table not registered: " + swp.tableName());
            }
            base = table.partitions().size() == 1
                    ? submitStreamingSingle(swp, table)   // phase 6d.1
                    : submitStreamingMulti(swp, table);   // phase 6d.2
        } else if (plan instanceof QueryPlanner.ShuffleJoinAggregatePlan sjap) {
            DistributedTable left = tables.get(sjap.leftTable());
            DistributedTable right = tables.get(sjap.rightTable());
            if (left == null || right == null) {
                throw new IllegalArgumentException("shuffle-join+agg needs both tables registered: "
                        + sjap.leftTable() + " + " + sjap.rightTable());
            }
            int width = ShufflePlacement.chooseWidth(agents.agentsWith(List.of("jvssql")).size(),
                    Math.min(Math.max(shuffleWidth, 1), shuffleWidthCap));
            base = submitShuffleJoinAggregate(sjap, left, right, width);
        } else if (plan instanceof QueryPlanner.ShuffleJoinPlan sjp) {
            DistributedTable left = tables.get(sjp.leftTable());
            DistributedTable right = tables.get(sjp.rightTable());
            if (left == null || right == null) {
                throw new IllegalArgumentException("shuffle-join needs both tables registered: "
                        + sjp.leftTable() + " + " + sjp.rightTable());
            }
            int width = ShufflePlacement.chooseWidth(agents.agentsWith(List.of("jvssql")).size(),
                    Math.min(Math.max(shuffleWidth, 1), shuffleWidthCap));
            base = submitShuffleJoin(sjp, left, right, width);
        } else {
            DistributedTable table = tables.get(plan.tableName());
            if (table == null) {
                throw new IllegalArgumentException("no distributed table registered under name: " + plan.tableName());
            }
            if (plan instanceof QueryPlanner.TwoStagePlan tsp) {
                int width = ShufflePlacement.chooseWidth(agents.agentsWith(List.of("jvssql")).size(),
                        Math.min(shuffleWidth, shuffleWidthCap));
                base = shuffleWidth > 0 && width > 1
                        ? submitTwoStageShuffle(tsp, table, width)
                        : submitTwoStage(tsp, table);
            } else {
                // Phase 5b.3: push LIMIT (and ORDER BY, if any) down to agents so each
                // partition returns at most N rows instead of scanning everything.
                // Only safe for SimplePlan — aggregate plans would truncate groups
                // mid-flight at partial stage, producing wrong final aggregates.
                String agentSql = plannerSql;
                if (!orderBy.isEmpty()) {
                    agentSql += " ORDER BY " + serializeOrderBy(orderBy);
                }
                if (limitOpt.isPresent()) {
                    agentSql += " LIMIT " + limitOpt.getAsLong();
                }
                // Phase 5b.2: for SimplePlan + ORDER BY, use N-way merge dispatch —
                // agents already sorted their partition (via the ORDER BY we pushed
                // down), driver streams the merged output row-by-row with O(N
                // partitions) memory instead of full-buffering everything.
                if (!orderBy.isEmpty()) {
                    base = submitSimpleMerged(agentSql, table, orderBy);
                    if (limitOpt.isPresent()) base = base.withLimit(limitOpt.getAsLong());
                    return base;
                }
                base = submitSimple(agentSql, table);
            }
        }
        // Post-processing: sort (phase 5b), then limit (phase 5a). ORDER BY here
        // only applies to non-Simple plans (TwoStage, ShuffleJoin) where we
        // still full-buffer sort — combine output all lands on one subject,
        // per-partition merge doesn't apply cleanly.
        if (!orderBy.isEmpty()) base = applyOrderBy(base, orderBy);
        return limitOpt.isPresent() ? base.withLimit(limitOpt.getAsLong()) : base;
    }

    /** Serialize {@code List<OrderKey>} back to SQL suitable for an agent-side ORDER BY. */
    private static String serializeOrderBy(List<QueryPlanner.OrderKey> keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sb.append(", ");
            var k = keys.get(i);
            sb.append(k.column()).append(k.direction() == QueryPlanner.OrderKey.Direction.DESC ? " DESC" : " ASC");
        }
        return sb.toString();
    }

    /**
     * Phase 5b.2: N-way merge dispatch for {@link QueryPlanner.SimplePlan}
     * with {@code ORDER BY}. Each partition sorts its rows locally
     * (agents receive `ORDER BY` in their SQL via
     * {@link #serializeOrderBy}); the driver keeps one head per partition
     * and picks the smallest via a priority queue. Streams the merged
     * output row-by-row — driver memory is O(partitions), not O(rows).
     *
     * <p>Falls back to buffered {@code applyOrderBy} for TwoStagePlan /
     * ShuffleJoinPlan where the per-partition boundary isn't as clean
     * (combine output all lands on one subject, not per-partition).</p>
     */
    private QueryHandle submitSimpleMerged(String agentSql, DistributedTable table,
                                           List<QueryPlanner.OrderKey> orderBy) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        List<DistributedTable.Partition> parts = table.partitions();

        // Per-partition queues + done flags. ROW routes to its partition's queue,
        // EOS marks that partition complete. Any partition ERROR immediately
        // yields a failing merge iterator.
        java.util.Map<String, LinkedBlockingQueue<JsonNode>> perPart = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Set<String> donePart = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicReference<String> errorRef = new java.util.concurrent.atomic.AtomicReference<>();
        for (var p : parts) perPart.put(p.key(), new LinkedBlockingQueue<>());

        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    LinkedBlockingQueue<JsonNode> q = perPart.get(m.partitionKey());
                    switch (m.kind()) {
                        case ROW   -> { if (q != null) q.add(m.row()); }
                        case EOS   -> donePart.add(m.partitionKey());
                        case ERROR -> errorRef.compareAndSet(null, m.errorMessage());
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            for (var p : parts) {
                Optional<AgentDescriptor> pick = agents.pick(p.requiredCapabilities());
                if (pick.isEmpty()) {
                    throw new IllegalStateException("no live agent has capabilities " + p.requiredCapabilities()
                            + " (needed for partition " + p.key() + " of table " + table.name() + ")");
                }
                AgentDescriptor a = pick.get();
                assignedAgents.add(a.agentId());
                TaskDescriptor td = new TaskDescriptor(
                        queryId, queryId + ":" + p.key(), 0,
                        table.name(), p.key(), agentSql,
                        p.requiredCapabilities(),
                        Subjects.result(queryId, p.key()));
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(td));
            }
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }

        RowSource mergeSource = mergeIterator(perPart, donePart, errorRef,
                new OrderComparator(orderBy), queryId, parts.size());
        return new QueryHandle(queryId, assignedAgents, mergeSource, resultSub, transport);
    }

    /**
     * Build the N-way merge iterator. Priority-queue keyed by the peeked
     * head-row of each partition; each {@code next()} call pops the
     * smallest row, advances that partition's cursor, re-inserts if the
     * partition still has rows to come.
     */
    private static RowSource mergeIterator(
            java.util.Map<String, LinkedBlockingQueue<JsonNode>> perPart,
            java.util.Set<String> donePart,
            java.util.concurrent.atomic.AtomicReference<String> errorRef,
            java.util.Comparator<JsonNode> cmp,
            String queryId,
            int totalPartitions) {

        class PartCursor {
            final String key;
            final LinkedBlockingQueue<JsonNode> queue;
            JsonNode head;
            PartCursor(String key, LinkedBlockingQueue<JsonNode> q) { this.key = key; this.queue = q; }
        }

        java.util.PriorityQueue<PartCursor> heap = new java.util.PriorityQueue<>(
                (a, b) -> cmp.compare(a.head, b.head));

        // Track whether we've done the initial fill (must happen inside next()
        // to respect the caller's timeout).
        boolean[] initialised = { false };

        return (timeout, unit) -> {
            String err = errorRef.get();
            if (err != null) throw new RuntimeException(err);

            if (!initialised[0]) {
                initialised[0] = true;
                for (var e : perPart.entrySet()) {
                    JsonNode first = pollUntilRowOrDone(e.getKey(), e.getValue(), donePart, errorRef, timeout, unit, queryId);
                    if (first != null) heap.offer(new PartCursor(e.getKey(), e.getValue()) {{ head = first; }});
                }
            }

            if (heap.isEmpty()) return null;   // every partition drained + done

            PartCursor top = heap.poll();
            JsonNode row = top.head;
            JsonNode next = pollUntilRowOrDone(top.key, top.queue, donePart, errorRef, timeout, unit, queryId);
            if (next != null) { top.head = next; heap.offer(top); }
            return row;
        };
    }

    /**
     * Wait for either a row on this partition's queue, or the partition to
     * be marked done. Returns the row, or {@code null} if the partition
     * is exhausted. Rechecks the error ref between polls so a mid-flight
     * failure aborts promptly.
     */
    private static JsonNode pollUntilRowOrDone(
            String partitionKey,
            LinkedBlockingQueue<JsonNode> q,
            java.util.Set<String> donePart,
            java.util.concurrent.atomic.AtomicReference<String> errorRef,
            long timeout, TimeUnit unit,
            String queryId) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            String err = errorRef.get();
            if (err != null) throw new RuntimeException(err);
            JsonNode row = q.poll(50, TimeUnit.MILLISECONDS);
            if (row != null) return row;
            if (donePart.contains(partitionKey) && q.isEmpty()) return null;
            if (System.nanoTime() > deadlineNanos) {
                throw new RuntimeException("timeout waiting for row on partition "
                        + partitionKey + " of query " + queryId);
            }
        }
    }

    /**
     * Post-process a base handle with a global sort. Full-buffers all rows,
     * sorts, returns a new handle wrapping a preloaded queue.
     *
     * <p>Works for every plan shape — simple scans (already sorted per
     * partition by jvssql), two-stage aggregates (whose combined output isn't
     * globally ordered), and shuffle-joins (whose per-bucket combine output
     * arrives in bucket order, not sort order). The base handle's own
     * subscription is closed when we're done draining.</p>
     */
    private QueryHandle applyOrderBy(QueryHandle base, List<QueryPlanner.OrderKey> orderBy) {
        List<JsonNode> rows;
        try {
            rows = base.collect(30, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted collecting rows for driver-side sort", ie);
        } finally {
            base.close();
        }
        rows.sort(new OrderComparator(orderBy));

        LinkedBlockingQueue<Object> outQ = new LinkedBlockingQueue<>();
        for (JsonNode r : rows) outQ.add(r);
        outQ.add(EOS_ALL);
        return new QueryHandle(base.queryId(), base.assignedAgents(), outQ, noopSubscription());
    }

    // -- simple plan ----------------------------------------------------------

    private QueryHandle submitSimple(String sql, DistributedTable table) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        DispatchResult r = dispatchAndCollect(sql, table, queryId, /*shuffleSpec*/ null);
        return new QueryHandle(queryId, r.assignedAgents, r.queue, r.sub, transport);
    }

    /**
     * Phase 6d.1 — single-partition streaming aggregate. Dispatches ONE
     * scan task with the ORIGINAL SQL; agent's jvssql runs the incremental
     * {@code StreamingAggregate} directly, per-window rows flow to driver
     * as watermark advances, driver forwards.
     */
    private QueryHandle submitStreamingSingle(QueryPlanner.StreamingWindowedPlan plan, DistributedTable table) {
        return submitSimple(plan.originalSql(), table);
    }

    /**
     * Phase 6d.2 — multi-partition streaming aggregate. Each partition runs
     * the PARTIAL SQL (still a windowed aggregate via jvssql streaming);
     * driver receives per-window partial rows and combines them across
     * partitions incrementally.
     *
     * <h4>Window-close detection (advance-past heuristic)</h4>
     * A window {@code W} is considered globally closed when every partition
     * has emitted at least one row for a strictly LATER window
     * ({@code window_start > W}). This works because jvssql's streaming
     * aggregate emits window {@code X}'s row only after watermark advances
     * past {@code X}'s end — if partition {@code P} emitted for window
     * {@code W+windowSize}, its watermark is at least {@code W+2·windowSize},
     * so {@code W} is definitively closed on {@code P}.
     *
     * <h4>Combine math</h4>
     * When a window closes globally, all buffered partial rows for that
     * window get registered into a small jvssql engine and the combine SQL
     * runs over them — same math as phase-2 combiner-at-driver
     * (SUM for COUNT/SUM, MIN for MIN, MAX for MAX, ratio for AVG). Output
     * rows are emitted to the user's queue.
     *
     * <h4>Caveats</h4>
     * <ul>
     *   <li>A partition that never emits (e.g. no events flowing) can
     *       stall window emission indefinitely. Explicit watermark
     *       heartbeats would fix this — not in the MVP wire protocol.</li>
     *   <li>On source-EOS or cancel, all remaining buffered windows are
     *       flushed unconditionally to the user, then EOS.</li>
     * </ul>
     */
    private QueryHandle submitStreamingMulti(QueryPlanner.StreamingWindowedPlan plan, DistributedTable table) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        List<DistributedTable.Partition> parts = table.partitions();
        int nParts = parts.size();

        // Ordered buffer: window_start → list of partial rows. TreeMap so
        // we can walk closed windows in ascending order and stop early.
        final java.util.TreeMap<Long, List<JsonNode>> buffered = new java.util.TreeMap<>();
        // Per-partition tracker: latest window_start observed. When min > W,
        // window W is globally closed.
        final java.util.Map<String, Long> partitionLatestWindow = new java.util.HashMap<>();
        for (var p : parts) partitionLatestWindow.put(p.key(), Long.MIN_VALUE);
        // EOS bookkeeping — one per partition.
        final java.util.Set<String> eosReceived = ConcurrentHashMap.newKeySet();

        LinkedBlockingQueue<Object> outQueue = new LinkedBlockingQueue<>();
        String windowCol = plan.groupColumns().isEmpty() ? null : plan.groupColumns().get(0);
        // Phase 6d.2.1: extract windowSize from WIN_START(field, N) so
        // WATERMARK messages can be converted to "latest closed window
        // start" for the same close-detection code path as row emissions.
        long windowSize = extractWindowSize(plan.originalSql());

        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    switch (m.kind()) {
                        case ROW -> {
                            JsonNode row = m.row();
                            String pk = m.partitionKey();
                            long ws = windowCol == null ? 0L : row.get(windowCol).asLong();
                            synchronized (buffered) {
                                buffered.computeIfAbsent(ws, k -> new ArrayList<>()).add(row);
                                Long prev = partitionLatestWindow.get(pk);
                                if (prev == null || ws > prev) partitionLatestWindow.put(pk, ws);
                                emitClosedWindows(buffered, partitionLatestWindow, plan, outQueue, /*drainAll*/ false);
                            }
                        }
                        case WATERMARK -> {
                            // Convert wm → highest closed window start.
                            // A window [W, W+size) is closed when wm >= W+size,
                            // so highest closed W = wm >= size ? ((wm - size) / size) * size : Long.MIN_VALUE.
                            if (windowSize <= 0) return;
                            long wm = m.watermarkMs();
                            long latestClosed = wm >= windowSize
                                    ? ((wm - windowSize) / windowSize) * windowSize
                                    : Long.MIN_VALUE;
                            if (latestClosed == Long.MIN_VALUE) return;
                            String pk = m.partitionKey();
                            synchronized (buffered) {
                                Long prev = partitionLatestWindow.get(pk);
                                if (prev == null || latestClosed > prev) {
                                    partitionLatestWindow.put(pk, latestClosed);
                                    emitClosedWindows(buffered, partitionLatestWindow, plan, outQueue, /*drainAll*/ false);
                                }
                            }
                        }
                        case EOS -> {
                            eosReceived.add(m.partitionKey());
                            if (eosReceived.size() >= nParts) {
                                synchronized (buffered) {
                                    emitClosedWindows(buffered, partitionLatestWindow, plan, outQueue, /*drainAll*/ true);
                                }
                                outQueue.add(EOS_ALL);
                            }
                        }
                        case ERROR -> {
                            outQueue.add(new QueryError(m.errorMessage()));
                            outQueue.add(EOS_ALL);
                        }
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            for (DistributedTable.Partition p : parts) {
                Optional<AgentDescriptor> pick = agents.pick(p.requiredCapabilities());
                if (pick.isEmpty()) {
                    throw new IllegalStateException("no live agent for capabilities " + p.requiredCapabilities());
                }
                AgentDescriptor a = pick.get();
                assignedAgents.add(a.agentId());
                TaskDescriptor scan = new TaskDescriptor(
                        queryId,
                        queryId + ":stream:" + p.key(),
                        0,
                        table.name(),
                        p.key(),
                        plan.partialSql(),
                        p.requiredCapabilities(),
                        Subjects.result(queryId, p.key()),
                        null,
                        null);
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(scan));
            }
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }
        return new QueryHandle(queryId, assignedAgents, outQueue, resultSub, transport);
    }

    /**
     * Walk buffered windows in ascending order; for each closed one, run
     * combine SQL over its buffered partials and push results onto the
     * output queue. Removes emitted entries from the buffer. Synchronized
     * on {@code buffered} by the caller.
     *
     * @param drainAll true on EOS — flush every remaining window unconditionally.
     */
    private void emitClosedWindows(java.util.TreeMap<Long, List<JsonNode>> buffered,
                                   java.util.Map<String, Long> partitionLatestWindow,
                                   QueryPlanner.StreamingWindowedPlan plan,
                                   LinkedBlockingQueue<Object> outQueue,
                                   boolean drainAll) {
        long minLatest = Long.MAX_VALUE;
        for (Long v : partitionLatestWindow.values()) {
            if (v == null || v == Long.MIN_VALUE) { minLatest = Long.MIN_VALUE; break; }
            if (v < minLatest) minLatest = v;
        }
        var it = buffered.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            long ws = entry.getKey();
            // "window ws is emittable when every partition has emitted for
            // window >= ws" → ws <= min(latest emitted per partition).
            // Missing partitions (Long.MIN_VALUE above) block closure until
            // they've contributed anything at all.
            boolean closed = drainAll || ws <= minLatest;
            if (!closed) return;   // TreeMap ascending → no later entry can be closed either
            List<JsonNode> partials = entry.getValue();
            it.remove();
            for (JsonNode combined : reduceOneWindow(partials, plan)) {
                outQueue.add(combined);
            }
        }
    }

    /**
     * Run combine SQL over the partial rows of a single closed window and
     * return the reduced rows (one per group-key within the window).
     * Uses a small jvssql engine — same math as phase-2 combiner-at-driver.
     */
    private List<JsonNode> reduceOneWindow(List<JsonNode> partials, QueryPlanner.StreamingWindowedPlan plan) {
        if (partials.isEmpty()) return List.of();
        Type partialType = synthesizePartialTypeFromRow(partials.get(0));
        try {
            JvsSqlEngine engine = JvsSqlEngine.builder()
                    .registerStream(QueryPlanner.COMBINE_INPUT_TABLE, jvsIterator(partials), partialType)
                    .build();
            PreparedQuery cq = engine.compile(plan.combineSql());
            List<JsonNode> out = new ArrayList<>();
            Iterator<JsonNode> rows = cq.asIterator();
            while (rows.hasNext()) out.add(rows.next());
            return out;
        } catch (Exception e) {
            throw new RuntimeException("streaming combine failed", e);
        }
    }

    /**
     * Phase 6d.2.1 — extract the tumbling-window size from a
     * {@code WIN_START(field, N)} call in the SQL. Returns 0 if no such
     * call is found (in which case the caller falls back to the
     * emission-only close heuristic).
     */
    private static final java.util.regex.Pattern WIN_START_ARG =
            java.util.regex.Pattern.compile(
                    "\\bWIN_START\\s*\\(\\s*[^,]+?,\\s*(\\d+)\\s*\\)",
                    java.util.regex.Pattern.CASE_INSENSITIVE);

    private static long extractWindowSize(String sql) {
        java.util.regex.Matcher m = WIN_START_ARG.matcher(sql);
        if (!m.find()) return 0L;
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * Best-effort type synthesis from the first partial row's fields.
     * Every field typed as {@code core_long} — sufficient because partial
     * aggregate outputs are all longs in the MVP (COUNT / SUM are long,
     * AVG decomposes to SUM+COUNT longs, MIN/MAX over long inputs stay
     * long). Same limitation as {@code synthesizePartialType} for phase-2.
     */
    private static Type synthesizePartialTypeFromRow(JsonNode first) {
        try {
            StringBuilder json = new StringBuilder();
            json.append("{\"name\":\"").append(QueryPlanner.COMBINE_INPUT_TABLE).append("\",\"fields\":[");
            boolean sep = false;
            for (var it = first.fieldNames(); it.hasNext(); ) {
                String n = it.next();
                if (sep) json.append(",");
                sep = true;
                json.append("{\"name\":\"").append(n).append("\",\"type\":\"core_long\"}");
            }
            json.append("]}");
            Type t = new Type();
            t.init(MAPPER.readTree(json.toString()));
            return t;
        } catch (Exception e) {
            throw new RuntimeException("failed to synthesize streaming partial type", e);
        }
    }

    /**
     * SQL-shaped comparator over JsonNode rows. Semantics:
     * <ul>
     *   <li>NULL / missing column sorts LAST for ASC, FIRST for DESC (standard SQL).</li>
     *   <li>Numeric-vs-numeric compares numerically (avoids "10" &lt; "9" trap).</li>
     *   <li>Mixed types (number vs. string) compares by textual representation
     *       as a fallback — same jvssql {@code compareValues} does.</li>
     *   <li>Multi-key: earlier keys dominate; ties fall through to later keys.</li>
     * </ul>
     */
    private static final class OrderComparator implements java.util.Comparator<JsonNode> {
        private final List<QueryPlanner.OrderKey> keys;
        OrderComparator(List<QueryPlanner.OrderKey> keys) { this.keys = keys; }

        @Override
        public int compare(JsonNode a, JsonNode b) {
            for (QueryPlanner.OrderKey k : keys) {
                JsonNode va = a == null ? null : a.get(k.column());
                JsonNode vb = b == null ? null : b.get(k.column());
                int c = compareOne(va, vb);
                if (c != 0) return k.direction() == QueryPlanner.OrderKey.Direction.DESC ? -c : c;
            }
            return 0;
        }

        private static int compareOne(JsonNode a, JsonNode b) {
            boolean aNull = a == null || a.isNull() || a.isMissingNode();
            boolean bNull = b == null || b.isNull() || b.isMissingNode();
            if (aNull && bNull) return 0;
            if (aNull) return 1;         // NULL sorts LAST for ASC — DESC flips it in the caller
            if (bNull) return -1;
            if (a.isNumber() && b.isNumber()) {
                return Double.compare(a.asDouble(), b.asDouble());
            }
            return a.asText().compareTo(b.asText());
        }
    }

    // -- two-stage combiner-at-driver (phase 2) --------------------------------

    private QueryHandle submitTwoStage(QueryPlanner.TwoStagePlan plan, DistributedTable table) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        DispatchResult r = dispatchAndCollect(plan.partialSql(), table, queryId, /*shuffleSpec*/ null);
        List<JsonNode> partialRows = new ArrayList<>();
        try {
            drainInto(r.queue, partialRows);
        } finally {
            r.sub.close();
        }
        Type partialType = synthesizePartialType(table.type(), plan);
        Iterator<JVS> partialAsJvs = jvsIterator(partialRows);
        JvsSqlEngine combineEngine = JvsSqlEngine.builder()
                .registerStream(QueryPlanner.COMBINE_INPUT_TABLE, partialAsJvs, partialType)
                .build();
        PreparedQuery cq = combineEngine.compile(plan.combineSql());

        LinkedBlockingQueue<Object> combinedQ = new LinkedBlockingQueue<>();
        Iterator<JsonNode> combined = cq.asIterator();
        while (combined.hasNext()) combinedQ.add(combined.next());
        combinedQ.add(EOS_ALL);
        return new QueryHandle(queryId, r.assignedAgents, combinedQ, noopSubscription());
    }

    // -- two-stage distributed combiner (phase 2.5.1) --------------------------

    private QueryHandle submitTwoStageShuffle(QueryPlanner.TwoStagePlan plan,
                                              DistributedTable table,
                                              int width) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        List<DistributedTable.Partition> parts = table.partitions();
        int expectedUpstream = parts.size();

        // Subscribe to the merged results subject BEFORE dispatching anything.
        // Combine workers will publish combined rows here, one row per group-key output.
        // Each combine worker sends EOS when it's done; we need `width` EOS-es to finish.
        AtomicInteger combineRemaining = new AtomicInteger(width);
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    switch (m.kind()) {
                        case ROW -> queue.add(m.row());
                        case EOS -> {
                            if (combineRemaining.decrementAndGet() == 0) queue.add(EOS_ALL);
                        }
                        case ERROR -> {
                            queue.add(new QueryError(m.errorMessage()));
                            queue.add(EOS_ALL);
                        }
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            // Step 1: dispatch N stage-1 combine tasks. Each subscribes to one bucket.
            List<AgentDescriptor> jvssqlAgents = agents.agentsWith(List.of("jvssql"));
            if (jvssqlAgents.isEmpty()) {
                throw new IllegalStateException("no live agent advertises 'jvssql' — can't run distributed combine");
            }
            String shufflePrefix = Subjects.SHUFFLE_PREFIX + queryId;
            for (int bucket = 0; bucket < width; bucket++) {
                AgentDescriptor a = jvssqlAgents.get(bucket % jvssqlAgents.size());   // round-robin
                assignedAgents.add(a.agentId());
                TaskDescriptor combineTask = new TaskDescriptor(
                        queryId,
                        queryId + ":combine:" + bucket,
                        1,                                    // stage 1
                        null, null,                           // no source table
                        plan.combineSql(),
                        Set.of("jvssql"),
                        Subjects.result(queryId, "combine-" + bucket),   // distinct sub-subject so the
                                                                        // wildcard subscription picks up each combine's rows
                        null,                                 // no shuffleSpec — combine tasks don't shuffle onward
                        new CombineSpec(
                                shufflePrefix + "." + bucket,        // subscribe to bucket subject
                                expectedUpstream,                     // wait for N partition EOS's
                                QueryPlanner.COMBINE_INPUT_TABLE));
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(combineTask));
            }

            // Step 2: give combine subscriptions time to establish before scans start
            // dumping rows. See SHUFFLE_READY_GRACE_MS Javadoc.
            try { Thread.sleep(SHUFFLE_READY_GRACE_MS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

            // Step 3: dispatch stage-0 scan-with-shuffle tasks.
            ShuffleSpec spec = new ShuffleSpec(plan.groupColumns(), width, shufflePrefix);
            for (DistributedTable.Partition p : parts) {
                Optional<AgentDescriptor> pick = agents.pick(p.requiredCapabilities());
                if (pick.isEmpty()) {
                    throw new IllegalStateException("no live agent has capabilities " + p.requiredCapabilities());
                }
                AgentDescriptor a = pick.get();
                assignedAgents.add(a.agentId());
                TaskDescriptor scanTask = new TaskDescriptor(
                        queryId,
                        queryId + ":" + p.key(),
                        0,
                        table.name(),
                        p.key(),
                        plan.partialSql(),
                        p.requiredCapabilities(),
                        Subjects.result(queryId, p.key()),   // unused when shuffleSpec is set — combines redirect
                        spec,
                        null);
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(scanTask));
            }
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }
        return new QueryHandle(queryId, assignedAgents, queue, resultSub, transport);
    }

    // -- phase 4b: shuffle-hash join ------------------------------------------

    /**
     * Dispatch a shuffle-hash join.
     *
     * <p>Both sides of the join get shuffled into the same N-bucket grid,
     * using the same hash function on their respective join keys. Combine
     * workers subscribe to matching buckets on BOTH sides, buffer both, and
     * run the original SQL against the buffered rows (with both tables
     * registered as local streams).</p>
     *
     * <p>Timing: dispatch combine tasks first, sleep briefly so subscriptions
     * establish, then dispatch scan tasks for both sides. Same pattern as
     * {@link #submitTwoStageShuffle}.</p>
     */
    private QueryHandle submitShuffleJoin(QueryPlanner.ShuffleJoinPlan plan,
                                          DistributedTable leftT,
                                          DistributedTable rightT,
                                          int width) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        int leftUpstream = leftT.partitions().size();
        int rightUpstream = rightT.partitions().size();

        AtomicInteger combineRemaining = new AtomicInteger(width);
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    switch (m.kind()) {
                        case ROW -> queue.add(m.row());
                        case EOS -> {
                            if (combineRemaining.decrementAndGet() == 0) queue.add(EOS_ALL);
                        }
                        case ERROR -> {
                            queue.add(new QueryError(m.errorMessage()));
                            queue.add(EOS_ALL);
                        }
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            List<AgentDescriptor> jvssqlAgents = agents.agentsWith(List.of("jvssql"));
            if (jvssqlAgents.isEmpty()) {
                throw new IllegalStateException("no live agent advertises 'jvssql' — can't run shuffle-hash join");
            }
            String shufflePrefix = Subjects.SHUFFLE_PREFIX + queryId;

            // Phase 4c.1: ship each side's Type as JSON so combine workers can
            // register an empty-but-typed stream when a bucket has zero rows on
            // that side. OUTER JOIN semantics need the schema of the empty side
            // to null-pad output rows; INNER JOIN doesn't need it (short-circuits
            // on empty) but we ship it unconditionally — same code path either way.
            String leftTypeJson = typeToJson(leftT.type());
            String rightTypeJson = typeToJson(rightT.type());

            // Step 1: N combine tasks, each subscribed to both left AND right
            // buckets. Combine SQL is the ORIGINAL query — the combine worker's
            // jvssql engine has both tables registered from its local buffers.
            for (int bucket = 0; bucket < width; bucket++) {
                AgentDescriptor a = jvssqlAgents.get(bucket % jvssqlAgents.size());
                assignedAgents.add(a.agentId());
                List<CombineSpec.InputSource> inputs = List.of(
                        new CombineSpec.InputSource(
                                shufflePrefix + ".left." + bucket, leftUpstream, plan.leftTable(), leftTypeJson),
                        new CombineSpec.InputSource(
                                shufflePrefix + ".right." + bucket, rightUpstream, plan.rightTable(), rightTypeJson));
                TaskDescriptor combineTask = new TaskDescriptor(
                        queryId,
                        queryId + ":join:" + bucket,
                        1,
                        null, null,
                        plan.originalSql(),
                        Set.of("jvssql"),
                        Subjects.result(queryId, "combine-" + bucket),
                        null,
                        new CombineSpec(inputs, plan.joinKind()));
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(combineTask));
            }

            // Step 2: grace period for subscriptions to establish before scan
            // tasks start publishing shuffled rows.
            try { Thread.sleep(SHUFFLE_READY_GRACE_MS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

            // Step 3: stage-0 scan tasks for LEFT side (with WHERE pushdown if applicable).
            String leftPd = QueryPlanner.computeSidePushdown(plan.originalSql(), plan.leftTable()).orElse(null);
            String rightPd = QueryPlanner.computeSidePushdown(plan.originalSql(), plan.rightTable()).orElse(null);
            ShuffleSpec leftSpec = new ShuffleSpec(
                    List.of(plan.leftKey()), width, shufflePrefix, "left");
            dispatchSideScans(queryId, leftT, plan.leftTable(), leftSpec, assignedAgents, leftPd);

            // Step 4: stage-0 scan tasks for RIGHT side (with WHERE pushdown if applicable).
            ShuffleSpec rightSpec = new ShuffleSpec(
                    List.of(plan.rightKey()), width, shufflePrefix, "right");
            dispatchSideScans(queryId, rightT, plan.rightTable(), rightSpec, assignedAgents, rightPd);
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }
        return new QueryHandle(queryId, assignedAgents, queue, resultSub, transport);
    }

    // -- phase 4b.2: shuffle-hash join + GROUP BY -----------------------------

    /**
     * Two-stage combine over a shuffle-hash join. Stage-0 shuffles both
     * sides by their join keys (same as phase 4b). Stage-1 per-bucket
     * combines run a PARTIAL SQL that both performs the join and computes
     * partial aggregates. Stage-2 driver-side combine reduces those partial
     * rows to final aggregates.
     *
     * <p>The stage-1 partial rows are keyed by GROUP BY cols and carry
     * aggregate-partial columns ({@code __c0__}, {@code __c1__}, ...);
     * the combine SQL sums/mins/maxes them back into user-visible aggregate
     * columns. Same rewrite pattern as phase-2 two-stage — the only
     * difference is the partial SQL's FROM clause carries the JOIN.</p>
     */
    private QueryHandle submitShuffleJoinAggregate(QueryPlanner.ShuffleJoinAggregatePlan plan,
                                                    DistributedTable leftT,
                                                    DistributedTable rightT,
                                                    int width) {
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        int leftUpstream = leftT.partitions().size();
        int rightUpstream = rightT.partitions().size();

        AtomicInteger combineRemaining = new AtomicInteger(width);
        LinkedBlockingQueue<Object> partialQueue = new LinkedBlockingQueue<>();
        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    switch (m.kind()) {
                        case ROW -> partialQueue.add(m.row());
                        case EOS -> {
                            if (combineRemaining.decrementAndGet() == 0) partialQueue.add(EOS_ALL);
                        }
                        case ERROR -> {
                            partialQueue.add(new QueryError(m.errorMessage()));
                            partialQueue.add(EOS_ALL);
                        }
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            List<AgentDescriptor> jvssqlAgents = agents.agentsWith(List.of("jvssql"));
            if (jvssqlAgents.isEmpty()) {
                throw new IllegalStateException("no live agent advertises 'jvssql' — can't run shuffle-hash join+agg");
            }
            String shufflePrefix = Subjects.SHUFFLE_PREFIX + queryId;
            String leftTypeJson = typeToJson(leftT.type());
            String rightTypeJson = typeToJson(rightT.type());

            // Stage-1 combines: each subscribes to both left+right buckets and
            // runs the PARTIAL SQL (join + partial-agg). Output rows carry
            // group cols + __cN__ partial aggregate columns.
            for (int bucket = 0; bucket < width; bucket++) {
                AgentDescriptor a = jvssqlAgents.get(bucket % jvssqlAgents.size());
                assignedAgents.add(a.agentId());
                List<CombineSpec.InputSource> inputs = List.of(
                        new CombineSpec.InputSource(
                                shufflePrefix + ".left." + bucket, leftUpstream, plan.leftTable(), leftTypeJson),
                        new CombineSpec.InputSource(
                                shufflePrefix + ".right." + bucket, rightUpstream, plan.rightTable(), rightTypeJson));
                TaskDescriptor combineTask = new TaskDescriptor(
                        queryId,
                        queryId + ":joinagg:" + bucket,
                        1,
                        null, null,
                        plan.partialSql(),
                        Set.of("jvssql"),
                        Subjects.result(queryId, "combine-" + bucket),
                        null,
                        new CombineSpec(inputs, plan.joinKind()));
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(combineTask));
            }

            try { Thread.sleep(SHUFFLE_READY_GRACE_MS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

            // Phase 4b.2.1: per-side WHERE pushdown. The WHERE is embedded
            // in the partial SQL (which the combines already run), so we can
            // extract single-side conjuncts from either the partialSql or an
            // upstream original — both carry the same WHERE clause.
            String leftPd = QueryPlanner.computeSidePushdown(plan.partialSql(), plan.leftTable()).orElse(null);
            String rightPd = QueryPlanner.computeSidePushdown(plan.partialSql(), plan.rightTable()).orElse(null);
            ShuffleSpec leftSpec = new ShuffleSpec(
                    List.of(plan.leftKey()), width, shufflePrefix, "left");
            dispatchSideScans(queryId, leftT, plan.leftTable(), leftSpec, assignedAgents, leftPd);
            ShuffleSpec rightSpec = new ShuffleSpec(
                    List.of(plan.rightKey()), width, shufflePrefix, "right");
            dispatchSideScans(queryId, rightT, plan.rightTable(), rightSpec, assignedAgents, rightPd);
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }

        // Drain partial rows, apply driver-side final combine. Same shape as
        // submitTwoStage's driver-side combine but with a group-col lookup
        // that falls through both left and right side types.
        List<JsonNode> partialRows = new ArrayList<>();
        try {
            drainInto(partialQueue, partialRows);
        } finally {
            resultSub.close();
        }
        Type partialType = synthesizePartialType(plan.groupColumns(), plan.partialColumns(),
                leftT.type(), rightT.type());
        Iterator<JVS> partialAsJvs = jvsIterator(partialRows);
        JvsSqlEngine combineEngine = JvsSqlEngine.builder()
                .registerStream(QueryPlanner.COMBINE_INPUT_TABLE, partialAsJvs, partialType)
                .build();
        PreparedQuery cq = combineEngine.compile(plan.combineSql());

        LinkedBlockingQueue<Object> combinedQ = new LinkedBlockingQueue<>();
        Iterator<JsonNode> combined = cq.asIterator();
        while (combined.hasNext()) combinedQ.add(combined.next());
        combinedQ.add(EOS_ALL);
        return new QueryHandle(queryId, assignedAgents, combinedQ, noopSubscription());
    }

    private void dispatchSideScans(String queryId, DistributedTable table, String tableName,
                                   ShuffleSpec spec, List<String> assignedAgents,
                                   String pushdownWhere) {
        for (DistributedTable.Partition p : table.partitions()) {
            Optional<AgentDescriptor> pick = agents.pick(p.requiredCapabilities());
            if (pick.isEmpty()) {
                throw new IllegalStateException("no live agent has capabilities " + p.requiredCapabilities()
                        + " (needed for " + tableName + " partition " + p.key() + ")");
            }
            AgentDescriptor a = pick.get();
            assignedAgents.add(a.agentId());
            // Phase 4b.2.1: per-side WHERE pushdown. If any conjunct of the
            // query's WHERE references only this side's columns, apply it at
            // the scan task before shuffle — cuts bandwidth for filtered
            // queries. Combine workers still see the full WHERE via the
            // original/partial SQL and re-apply harmlessly.
            String scanSql = "SELECT * FROM " + tableName;
            if (pushdownWhere != null && !pushdownWhere.isBlank()) {
                scanSql += " WHERE " + pushdownWhere;
            }
            TaskDescriptor scanTask = new TaskDescriptor(
                    queryId,
                    queryId + ":" + tableName + ":" + p.key(),
                    0,
                    tableName,
                    p.key(),
                    scanSql,
                    p.requiredCapabilities(),
                    Subjects.result(queryId, tableName + "-" + p.key()),   // unused when shuffleSpec set
                    spec,
                    null);
            transport.publish(Subjects.task(a.agentId()), Codecs.encode(scanTask));
        }
    }

    // -- shared dispatch helper (phase 1 + phase 2 partial + shuffle-scan) ----

    private record DispatchResult(List<String> assignedAgents,
                                  LinkedBlockingQueue<Object> queue,
                                  MeshTransport.Subscription sub) {}

    private DispatchResult dispatchAndCollect(String taskSql, DistributedTable table,
                                              String queryId, ShuffleSpec shuffleSpec) {
        List<DistributedTable.Partition> parts = table.partitions();
        AtomicInteger remaining = new AtomicInteger(parts.size());
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();

        MeshTransport.Subscription resultSub = transport.subscribe(
                Subjects.resultsForQuery(queryId),
                bytes -> {
                    ResultMessage m = Codecs.decode(bytes, ResultMessage.class);
                    switch (m.kind()) {
                        case ROW -> queue.add(m.row());
                        case EOS -> {
                            if (remaining.decrementAndGet() == 0) queue.add(EOS_ALL);
                        }
                        case ERROR -> {
                            queue.add(new QueryError(m.errorMessage()));
                            queue.add(EOS_ALL);
                        }
                    }
                });

        List<String> assignedAgents = new ArrayList<>();
        try {
            for (DistributedTable.Partition p : parts) {
                Optional<AgentDescriptor> pick = agents.pick(p.requiredCapabilities());
                if (pick.isEmpty()) {
                    throw new IllegalStateException("no live agent has capabilities " + p.requiredCapabilities()
                            + " (needed for partition " + p.key() + " of table " + table.name() + ")");
                }
                AgentDescriptor a = pick.get();
                assignedAgents.add(a.agentId());
                TaskDescriptor td = new TaskDescriptor(
                        queryId,
                        queryId + ":" + p.key(),
                        0,
                        table.name(),
                        p.key(),
                        taskSql,
                        p.requiredCapabilities(),
                        Subjects.result(queryId, p.key()),
                        shuffleSpec,
                        null);
                transport.publish(Subjects.task(a.agentId()), Codecs.encode(td));
            }
        } catch (RuntimeException e) {
            resultSub.close();
            throw e;
        }
        return new DispatchResult(assignedAgents, queue, resultSub);
    }

    private static void drainInto(LinkedBlockingQueue<Object> queue, List<JsonNode> out) {
        try {
            while (true) {
                Object o = queue.poll(10, TimeUnit.SECONDS);
                if (o == null) throw new RuntimeException("timeout waiting for partial rows");
                if (o == EOS_ALL) return;
                if (o instanceof QueryError err) throw new RuntimeException(err.message());
                out.add((JsonNode) o);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted collecting partial rows", ie);
        }
    }

    private Type synthesizePartialType(Type sourceType, QueryPlanner.TwoStagePlan plan) {
        return synthesizePartialType(plan.groupColumns(), plan.partialColumns(), sourceType);
    }

    /**
     * Build a JVS Type describing the shape of stage-1 partial rows —
     * group cols followed by {@code __cN__} partial-aggregate cols. Group-col
     * type is looked up across {@code sources} in order (falls back to
     * {@code core_string} if none has that field); the shuffle-join
     * aggregate case passes both left and right table types so group cols
     * from either side resolve.
     */
    private Type synthesizePartialType(List<String> groupColumns,
                                       List<QueryPlanner.PartialColumn> partialColumns,
                                       Type... sources) {
        try {
            StringBuilder json = new StringBuilder();
            json.append("{\"name\":\"").append(QueryPlanner.COMBINE_INPUT_TABLE).append("\",\"fields\":[");
            boolean first = true;
            for (String g : groupColumns) {
                if (!first) json.append(",");
                first = false;
                String t = lookupTypeName(g, sources);
                json.append("{\"name\":\"").append(g).append("\",\"type\":\"").append(t).append("\"}");
            }
            // Every partial-aggregate output is core_long in the MVP: COUNT is
            // always long; SUM/MIN/MAX over the demo dataset's core_long columns
            // stay long; AVG produces two partial columns (SUM + COUNT) both long.
            // If a source column is double / decimal, upgrade this to look up
            // the underlying field's type — deferred phase-3.5 item.
            for (QueryPlanner.PartialColumn p : partialColumns) {
                if (!first) json.append(",");
                first = false;
                json.append("{\"name\":\"").append(p.alias()).append("\",\"type\":\"core_long\"}");
            }
            json.append("]}");
            Type t = new Type();
            t.init(MAPPER.readTree(json.toString()));
            return t;
        } catch (Exception e) {
            throw new RuntimeException("failed to synthesize combine input type", e);
        }
    }

    private static String lookupTypeName(String colName, Type... sources) {
        for (Type s : sources) {
            if (s == null) continue;
            try {
                var f = s.getField(colName);
                if (f != null && f.getType() != null && f.getType().getName() != null) {
                    return f.getType().getName();
                }
            } catch (Exception ignore) {}
        }
        return "core_string";
    }

    private static Iterator<JVS> jvsIterator(List<JsonNode> rows) {
        return new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < rows.size(); }
            @Override public JVS next() { return new JVS(rows.get(i++)); }
        };
    }

    private static MeshTransport.Subscription noopSubscription() {
        return () -> {};
    }

    /**
     * Serialize a Type's underlying JSON definition for wire transport. Used
     * by the shuffle-join dispatcher to ship per-side schemas to combine
     * workers — needed for OUTER JOIN semantics where an empty bucket still
     * has to null-pad its columns. Falls back to a minimal name-only stub
     * if the Type wasn't initialized from a JsonNode (defensive; shouldn't
     * happen in practice).
     */
    private static String typeToJson(Type type) {
        if (type == null) return null;
        JsonNode node = type.getNode();
        if (node != null) return node.toString();
        return "{\"name\":\"" + type.getName() + "\",\"fields\":[]}";
    }

    // -- QueryHandle -----------------------------------------------------------

    private static final Object EOS_ALL = new Object();

    private record QueryError(String message) {}

    /**
     * Pluggable row source. Two impls: queue-based (phase 1+) and
     * N-way merge-based (phase 5b.2). Both yield rows one at a time
     * with a timeout, return {@code null} on end-of-stream, throw on error.
     */
    @FunctionalInterface
    interface RowSource {
        /** Return the next row, {@code null} on EOS. Throws on error / timeout. */
        JsonNode next(long timeout, TimeUnit unit) throws InterruptedException;
    }

    /** Queue-based row source — pulls from a single shared LinkedBlockingQueue. */
    private static RowSource queueSource(String queryId, LinkedBlockingQueue<Object> queue) {
        return (timeout, unit) -> {
            Object next = queue.poll(timeout, unit);
            if (next == null) throw new RuntimeException("timeout waiting for row on query " + queryId);
            if (next == EOS_ALL) return null;
            if (next instanceof QueryError err) throw new RuntimeException(err.message());
            return (JsonNode) next;
        };
    }

    public static final class QueryHandle implements AutoCloseable {
        private final String queryId;
        private final List<String> agents;
        private final RowSource source;
        private final MeshTransport.Subscription sub;
        /**
         * Transport reference for publishing {@link com.hitorro.mesh.CancelMessage}
         * on {@link #close()}. Nullable — post-processing handles (created by
         * {@code applyOrderBy}) don't need it because they've already drained
         * the base handle and no agents are still running for their queryId.
         */
        private final MeshTransport transport;
        private long remaining = Long.MAX_VALUE;   // phase 5a: LIMIT cap
        private boolean closed;

        QueryHandle(String queryId, List<String> agents,
                    LinkedBlockingQueue<Object> queue,
                    MeshTransport.Subscription sub,
                    MeshTransport transport) {
            this(queryId, agents, queueSource(queryId, queue), sub, transport);
        }

        /** Legacy 4-arg constructor for internal call sites that don't dispatch to agents. */
        QueryHandle(String queryId, List<String> agents,
                    LinkedBlockingQueue<Object> queue,
                    MeshTransport.Subscription sub) {
            this(queryId, agents, queue, sub, null);
        }

        /** Pluggable-source constructor (phase 5b.2 N-way merge). */
        QueryHandle(String queryId, List<String> agents,
                    RowSource source,
                    MeshTransport.Subscription sub,
                    MeshTransport transport) {
            this.queryId = queryId;
            this.agents = List.copyOf(agents);
            this.source = source;
            this.sub = sub;
            this.transport = transport;
        }

        public String queryId() { return queryId; }
        public List<String> assignedAgents() { return agents; }

        /**
         * Cap the total number of rows this handle will yield to {@code n}.
         * After {@code n} rows, {@link #nextRow} returns {@code null} as if the
         * stream had ended. Phase-5a driver-side LIMIT — agents may still be
         * doing work and shipping rows, but the caller sees at most {@code n}.
         */
        public QueryHandle withLimit(long n) {
            this.remaining = Math.max(0, n);
            return this;
        }

        public JsonNode nextRow(long timeout, TimeUnit unit) throws InterruptedException {
            if (remaining <= 0) return null;
            JsonNode row = source.next(timeout, unit);
            if (row == null) return null;
            remaining--;
            return row;
        }

        public List<JsonNode> collect(long timeout, TimeUnit unit) throws InterruptedException {
            List<JsonNode> out = new ArrayList<>();
            JsonNode r;
            while ((r = nextRow(timeout, unit)) != null) out.add(r);
            return out;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            // Phase 6c.2: publish cancel BEFORE closing the subscription — that
            // way if the driver receives an EOS from a cancelled agent, we
            // still process it cleanly. Only publish if the handle owns a
            // transport (agent-facing handles do; post-processing wrappers don't).
            if (transport != null) {
                try {
                    transport.publish(com.hitorro.mesh.Subjects.control(queryId),
                            com.hitorro.mesh.Codecs.encode(
                                    new com.hitorro.mesh.CancelMessage(queryId)));
                } catch (Throwable ignore) {
                    // Best-effort: cancel is an optimization for streaming queries.
                    // Batch queries that finish naturally never send it either.
                }
            }
            sub.close();
        }
    }
}
