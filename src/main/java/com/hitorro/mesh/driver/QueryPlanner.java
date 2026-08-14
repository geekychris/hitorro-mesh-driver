/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits a SQL query into a per-partition <b>partial</b> plan (runs on
 * every agent that holds a partition of the source table) and an optional
 * driver-side (or combine-worker-side) <b>combine</b> plan (runs once
 * over the union of partial results).
 *
 * <h3>Two plan shapes</h3>
 * <ul>
 *   <li>{@link SimplePlan} — projection-only queries where partial output
 *       IS the final result. Union at the driver, no combine step.</li>
 *   <li>{@link TwoStagePlan} — {@code SELECT ... FROM t [WHERE...]
 *       GROUP BY ...} or {@code SELECT DISTINCT ... FROM t [WHERE...]},
 *       optionally with a HAVING clause. Aggregates supported:
 *       COUNT / SUM / MIN / MAX (associative + commutative, combine directly)
 *       and AVG (decomposed to SUM + COUNT partials, combined as
 *       {@code SUM(sums) / SUM(counts)}).</li>
 * </ul>
 *
 * <h3>Aggregate rules</h3>
 *
 * <table>
 *   <caption>Partial + combine expressions per source aggregate</caption>
 *   <tr><th>Source aggregate</th><th>Partial columns</th><th>Combine expr</th></tr>
 *   <tr><td>{@code COUNT(*)}</td>       <td>{@code __c<i>N</i>__ : COUNT(*)}</td>       <td>{@code SUM(__c<i>N</i>__)}</td></tr>
 *   <tr><td>{@code SUM(x)}</td>         <td>{@code __c<i>N</i>__ : SUM(x)}</td>         <td>{@code SUM(__c<i>N</i>__)}</td></tr>
 *   <tr><td>{@code MIN(x)}</td>         <td>{@code __c<i>N</i>__ : MIN(x)}</td>         <td>{@code MIN(__c<i>N</i>__)}</td></tr>
 *   <tr><td>{@code MAX(x)}</td>         <td>{@code __c<i>N</i>__ : MAX(x)}</td>         <td>{@code MAX(__c<i>N</i>__)}</td></tr>
 *   <tr><td>{@code AVG(x)}</td>         <td>{@code __c<i>N</i>__ : SUM(x)}, {@code __c<i>N+1</i>__ : COUNT(x)}</td>  <td>{@code 1.0 * SUM(__c<i>N</i>__) / SUM(__c<i>N+1</i>__)}</td></tr>
 * </table>
 *
 * <h3>HAVING</h3>
 * <p>Extracted from the source query and rewritten so aggregate references
 * become their combine expressions. Applied at the combine step.</p>
 *
 * <h3>SELECT DISTINCT</h3>
 * <p>Rewritten to {@code GROUP BY <all selected columns>} with no
 * aggregates — the combine step naturally deduplicates across partitions.</p>
 *
 * <h3>Why regex parsing</h3>
 * <p>Regex feels wrong for SQL, but the alternative is duplicating jvssql's
 * Calcite parser here. The agent parses the same SQL with the real Calcite
 * pipeline; malformed input fails there with a real parser error. This
 * planner exists to detect shape, extract group cols + aggregates + HAVING,
 * and rewrite. Replace with a Calcite-based planner when we need
 * subqueries in FROM, multi-table sources, or CTEs.</p>
 */
public final class QueryPlanner {

    private QueryPlanner() {}

    // -- shapes we can't distribute ------------------------------------------

    /**
     * Shapes we can never rewrite for distribution. JOIN was here until
     * phase 4a — we now allow JOINs whose right side is a registered broadcast
     * table (small dimension table replicated to every agent). Any JOIN whose
     * right table isn't broadcast still fails, via {@link #validateJoins}.
     */
    private static final Pattern HARD_DISALLOWED =
            Pattern.compile("\\b(UNION)\\b",
                    Pattern.CASE_INSENSITIVE);

    /**
     * Extracts the numeric argument of a trailing {@code LIMIT N} clause.
     * Case-insensitive. Only supports the simple {@code LIMIT N} form —
     * {@code LIMIT N OFFSET M} isn't handled. Phase 5a scope.
     */
    private static final Pattern LIMIT_CLAUSE =
            Pattern.compile("\\bLIMIT\\s+(\\d+)\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * Extracts an {@code ORDER BY} clause (allowing an optional trailing
     * LIMIT). Phase 5b — used by the driver to build a comparator for the
     * final result stream.
     */
    private static final Pattern ORDER_BY_CLAUSE =
            Pattern.compile("\\bORDER\\s+BY\\s+(.+?)(?=$|\\bLIMIT\\b)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern JOIN_TABLE =
            Pattern.compile("\\bJOIN\\s+([A-Za-z_][A-Za-z0-9_.]*)",
                    Pattern.CASE_INSENSITIVE);

    /**
     * Captures the full JOIN chain (each JOIN keyword + table + ON clause,
     * potentially several concatenated) so the partial-SQL builder can
     * preserve it verbatim after {@code FROM &lt;table&gt;}.
     */
    private static final Pattern JOIN_CHAIN =
            Pattern.compile("\\b((?:JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|INNER\\s+JOIN)\\s+.+?)"
                    + "(?=$|\\bWHERE\\b|\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bHAVING\\b|\\bLIMIT\\b)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // -- query anatomy patterns ----------------------------------------------

    private static final Pattern FROM_TABLE =
            Pattern.compile("\\bFROM\\s+([A-Za-z_][A-Za-z0-9_.]*)", Pattern.CASE_INSENSITIVE);
    /**
     * Capture everything after {@code GROUP BY} up to the next clause boundary.
     * {@code .+?} (not a character class) so function-call group cols like
     * {@code WIN_START(event_time, 60000)} are captured — {@link #parseGroupCols}
     * handles the parenthesis-aware split. Phase 6d.
     */
    private static final Pattern HAS_GROUP_BY =
            Pattern.compile("\\bGROUP\\s+BY\\s+(.+?)(?=$|\\bORDER\\s+BY\\b|\\bLIMIT\\b|\\bHAVING\\b)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_LIST =
            Pattern.compile("\\bSELECT\\s+(?:(DISTINCT)\\s+)?(.+?)\\bFROM\\b",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern WHERE_CLAUSE =
            Pattern.compile("\\bWHERE\\s+(.+?)(?=$|\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bHAVING\\b|\\bLIMIT\\b)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HAVING_CLAUSE =
            Pattern.compile("\\bHAVING\\s+(.+?)(?=$|\\bORDER\\s+BY\\b|\\bLIMIT\\b)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern AGG_CALL =
            Pattern.compile("\\b(COUNT|SUM|MIN|MAX|AVG)\\s*\\(\\s*(\\*|[^)]+?)\\s*\\)",
                    Pattern.CASE_INSENSITIVE);

    /** Marker interface — {@link SimplePlan}, {@link TwoStagePlan}, {@link ShuffleJoinPlan}, or {@link ShuffleJoinAggregatePlan}. */
    public sealed interface Plan permits SimplePlan, TwoStagePlan, ShuffleJoinPlan, ShuffleJoinAggregatePlan {
        String tableName();
    }

    /** Union-at-driver plan for pure projection/filter queries. */
    public record SimplePlan(String tableName) implements Plan {}

    /**
     * Shuffle-hash join plan (phase 4b): both sides are distributed tables
     * joined by an equijoin on shuffle keys. Stage-0 for each side does a
     * full-partition scan and hash-partitions rows by its side's join key
     * into N buckets. Stage-1 combine workers subscribe to matching buckets
     * on both sides, buffer, and run the original SQL locally (both tables
     * registered in the combine worker's jvssql engine).
     */
    public record ShuffleJoinPlan(
            String tableName,        // left table name — for DistributedTable lookup
            String leftTable,
            String rightTable,
            String leftKey,
            String rightKey,
            String originalSql,      // combine workers run this verbatim
            com.hitorro.mesh.CombineSpec.JoinKind joinKind
    ) implements Plan {}

    /**
     * Phase 4b.2: shuffle-hash join + GROUP BY / aggregates. Both sides are
     * distributed tables. Stage-0 shuffles both by their join keys (same as
     * phase 4b). Stage-1 combine workers run a PARTIAL SQL that both
     * performs the JOIN and computes partial aggregates per bucket
     * (COUNT/SUM/MIN/MAX/AVG all decompose to reduce-friendly partials —
     * same rewrite as phase 2's TwoStagePlan). Stage-2 driver-side combine
     * reduces the partial rows across buckets to final aggregate rows.
     *
     * <p>The plan doubles as both a shuffle-join plan and a two-stage plan —
     * it carries the metadata both dispatch paths need. The partial and
     * combine SQL are constructed by the same {@link #planTwoStage} helper
     * that handles phase-2 aggregates; only the FROM/JOIN chain differs.</p>
     */
    public record ShuffleJoinAggregatePlan(
            String tableName,                     // left table name
            String leftTable,
            String rightTable,
            String leftKey,
            String rightKey,
            com.hitorro.mesh.CombineSpec.JoinKind joinKind,
            String partialSql,                    // per-bucket combine SQL — the join + partial-agg
            String combineSql,                    // driver-side final combine SQL
            List<String> groupColumns,
            List<PartialColumn> partialColumns
    ) implements Plan {}

    /**
     * Two-stage plan.
     *
     * @param tableName        source table
     * @param partialSql       SQL each agent runs against its local partition —
     *                         columns are the group cols followed by aliased
     *                         partial aggregate columns
     * @param combineSql       SQL the driver/combine-worker runs over the union
     *                         of partial rows, referencing the ephemeral table
     *                         name {@link #COMBINE_INPUT_TABLE}
     * @param groupColumns     column names in group-by order (also the leading
     *                         cols of partial output)
     * @param partialColumns   the __c0__, __c1__, ... alias names of partial
     *                         aggregate columns. Combine input's type synthesis
     *                         uses these (plus group cols) as its schema.
     */
    public record TwoStagePlan(
            String tableName,
            String partialSql,
            String combineSql,
            List<String> groupColumns,
            List<PartialColumn> partialColumns
    ) implements Plan {}

    /** One partial-stage output column — alias + the SQL fragment that produces it. */
    public record PartialColumn(String alias, String partialSqlFragment) {}

    /** Ephemeral table name the combine plan reads from — bound at execution time. */
    public static final String COMBINE_INPUT_TABLE = "__mesh_partial__";

    // -- entry point ---------------------------------------------------------

    /** Phase 1-3 compatibility overload — no broadcast, no distributed-table awareness. */
    public static Plan plan(String sql) {
        return plan(sql, java.util.Set.of(), java.util.Set.of());
    }

    /** Phase 4a compatibility overload — broadcast only, no shuffle-join. */
    public static Plan plan(String sql, java.util.Set<String> broadcastTables) {
        return plan(sql, broadcastTables, java.util.Set.of());
    }

    /**
     * @param broadcastTables    names of tables replicated to every agent —
     *                           JOIN allowed against these
     * @param distributedTables  names of registered distributed tables — JOIN
     *                           between two of them triggers the phase-4b
     *                           shuffle-hash path
     */
    public static Plan plan(String sql,
                            java.util.Set<String> broadcastTables,
                            java.util.Set<String> distributedTables) {
        Matcher hard = HARD_DISALLOWED.matcher(sql);
        if (hard.find()) {
            throw new IllegalArgumentException(
                    "mesh distribution does not yet support: " + hard.group(1)
                    + " — run this query on a single jvssql engine locally. sql=" + sql);
        }

        // Phase 4b: detect fact-x-fact join BEFORE the broadcast-only guard.
        java.util.Optional<Plan> shuffleJoin =
                tryPlanShuffleJoin(sql, distributedTables);
        if (shuffleJoin.isPresent()) return shuffleJoin.get();

        validateJoins(sql, broadcastTables);

        Matcher from = FROM_TABLE.matcher(sql);
        if (!from.find()) {
            throw new IllegalArgumentException("could not identify a source table in FROM clause: " + sql);
        }
        String table = from.group(1);

        Matcher selList = SELECT_LIST.matcher(sql);
        if (!selList.find()) {
            throw new IllegalArgumentException("could not parse SELECT list: " + sql);
        }
        boolean distinct = selList.group(1) != null;
        String selectList = selList.group(2);
        Matcher groupByM = HAS_GROUP_BY.matcher(sql);

        // SELECT DISTINCT with no GROUP BY → rewrite to GROUP BY <all selected columns>.
        // Combine step naturally dedups across partitions.
        if (distinct && !groupByM.find()) {
            List<String> cols = splitTopLevel(selectList);
            String rewritten = "SELECT " + String.join(", ", cols)
                    + " FROM " + table
                    + extractWhere(sql).map(w -> " WHERE " + w).orElse("")
                    + " GROUP BY " + String.join(", ", cols);
            return planTwoStage(rewritten, table, cols, /*sourceHaving*/ null);
        }

        // Aggregate present without GROUP BY → not supported.
        if (!groupByM.reset().find()) {
            if (AGG_CALL.matcher(selectList).find()) {
                throw new IllegalArgumentException(
                        "aggregate without GROUP BY not yet supported for distribution — "
                        + "either group by a column or run locally. sql=" + sql);
            }
            return new SimplePlan(table);
        }

        // Standard GROUP BY path.
        List<String> groupCols = parseGroupCols(groupByM.group(1).trim());
        Matcher havingM = HAVING_CLAUSE.matcher(sql);
        String having = havingM.find() ? havingM.group(1).trim() : null;
        return planTwoStage(sql, table, groupCols, having);
    }

    // -- two-stage planning --------------------------------------------------

    private static TwoStagePlan planTwoStage(String sql, String table, List<String> groupCols, String sourceHaving) {
        Matcher selList = SELECT_LIST.matcher(sql);
        if (!selList.find()) {
            throw new IllegalArgumentException("could not parse SELECT list: " + sql);
        }
        String select = selList.group(2);

        // Extract each aggregate call from the SELECT list, in order. For each,
        // record (a) the partial-column fragments it needs and (b) the combine
        // expression that reduces those partials. This mapping is also used to
        // rewrite the HAVING clause below.
        List<PartialColumn> partialCols = new ArrayList<>();
        List<CombineDef> combineDefs = new ArrayList<>();
        /**
         * source-aggregate-text → combine-expression. Used to rewrite HAVING.
         * e.g. "COUNT(*)" → "SUM(__c0__)", "AVG(x)" → "1.0 * SUM(__c0__) / SUM(__c1__)".
         */
        Map<String, String> aggToCombine = new LinkedHashMap<>();

        Matcher aggIt = AGG_CALL.matcher(select);
        int partialIdx = 0;
        int outputIdx = 0;
        while (aggIt.find()) {
            String func = aggIt.group(1).toUpperCase();
            String arg = aggIt.group(2);
            String sourceAggText = aggIt.group();      // exact text, for HAVING rewrite
            switch (func) {
                case "COUNT", "SUM", "MIN", "MAX" -> {
                    String alias = "__c" + partialIdx + "__";
                    partialCols.add(new PartialColumn(alias, func + "(" + arg + ") AS " + alias));
                    String reducer = switch (func) {
                        case "COUNT", "SUM" -> "SUM(" + alias + ")";
                        case "MIN"          -> "MIN(" + alias + ")";
                        case "MAX"          -> "MAX(" + alias + ")";
                        default -> throw new IllegalStateException(func);
                    };
                    combineDefs.add(new CombineDef(reducer, "c" + outputIdx));
                    aggToCombine.put(sourceAggText, reducer);
                    partialIdx++;
                }
                case "AVG" -> {
                    String sumAlias = "__c" + partialIdx + "__";
                    String cntAlias = "__c" + (partialIdx + 1) + "__";
                    partialCols.add(new PartialColumn(sumAlias, "SUM(" + arg + ") AS " + sumAlias));
                    partialCols.add(new PartialColumn(cntAlias, "COUNT(" + arg + ") AS " + cntAlias));
                    String reducer = "1.0 * SUM(" + sumAlias + ") / SUM(" + cntAlias + ")";
                    combineDefs.add(new CombineDef(reducer, "c" + outputIdx));
                    aggToCombine.put(sourceAggText, reducer);
                    partialIdx += 2;
                }
                default -> throw new IllegalStateException("unhandled aggregate: " + func);
            }
            outputIdx++;
        }

        // Phase 6d: each group col has both an expression (evaluated by jvssql
        // in the partial) and a name (used in the partial output + combine).
        // Simple column refs use the bare name for both — no schema change,
        // no client-visible column rename. Function-call group cols
        // (e.g. WIN_START(event_time, 60000)) get an auto-alias g0, g1, ...
        // because the raw expression can't survive as a column identifier.
        //
        // Table qualifier stripping (docs.lang → lang) still applies to simple
        // refs — see the phase-4a decision below on why qualifiers break
        // jvssql GROUP BY silently.
        List<GroupCol> groupPlan = new ArrayList<>();
        int autoAliasIdx = 0;
        for (String g : groupCols) {
            if (isSimpleColumnRef(g)) {
                int dot = g.lastIndexOf('.');
                String flat = dot >= 0 ? g.substring(dot + 1) : g;
                groupPlan.add(new GroupCol(flat, flat));   // partial expr == combine name
            } else {
                groupPlan.add(new GroupCol(g, "g" + autoAliasIdx++));
            }
        }

        // Partial SQL: SELECT <expr AS name | name>, partial-aggs FROM table [WHERE ...] GROUP BY <expr | name>
        // Comma-safe against empty groupPlan (global aggregate case: no
        // GROUP BY, all output is partial aggregates).
        StringBuilder partialSql = new StringBuilder();
        partialSql.append("SELECT ");
        boolean sep = false;
        for (GroupCol g : groupPlan) {
            if (sep) partialSql.append(", ");
            partialSql.append(g.partialExpr.equals(g.combineName)
                    ? g.combineName
                    : g.partialExpr + " AS " + g.combineName);
            sep = true;
        }
        for (PartialColumn p : partialCols) {
            if (sep) partialSql.append(", ");
            partialSql.append(p.partialSqlFragment());
            sep = true;
        }
        partialSql.append(" FROM ").append(table);
        extractJoinChain(sql).ifPresent(j -> partialSql.append(" ").append(j));
        extractWhere(sql).ifPresent(w -> partialSql.append(" WHERE ").append(w));
        if (!groupPlan.isEmpty()) {
            partialSql.append(" GROUP BY ");
            for (int i = 0; i < groupPlan.size(); i++) {
                if (i > 0) partialSql.append(", ");
                // For a function-call group col, use the expression (jvssql needs
                // the actual computation, not a column reference to itself).
                // For a simple ref, the expression IS the name.
                partialSql.append(groupPlan.get(i).partialExpr);
            }
        }

        // Combine SQL: SELECT <combineName>, combine-defs FROM __mesh_partial__ GROUP BY <combineName> [HAVING rewritten]
        // Same comma-safe pattern as the partial SQL above.
        List<String> combineNames = groupPlan.stream().map(GroupCol::combineName).toList();
        StringBuilder combineSql = new StringBuilder();
        combineSql.append("SELECT ");
        boolean combineSep = false;
        for (String n : combineNames) {
            if (combineSep) combineSql.append(", ");
            combineSql.append(n);
            combineSep = true;
        }
        for (CombineDef d : combineDefs) {
            if (combineSep) combineSql.append(", ");
            combineSql.append(d.expr()).append(" AS ").append(d.alias());
            combineSep = true;
        }
        combineSql.append(" FROM ").append(COMBINE_INPUT_TABLE);
        if (!combineNames.isEmpty()) {
            combineSql.append(" GROUP BY ");
            appendJoin(combineSql, combineNames);
        }
        if (sourceHaving != null) {
            combineSql.append(" HAVING ").append(rewriteHaving(sourceHaving, aggToCombine));
        }

        return new TwoStagePlan(table, partialSql.toString(), combineSql.toString(), combineNames, partialCols);
    }

    /** One group column's split representation — expression in partial, name in combine. */
    private record GroupCol(String partialExpr, String combineName) {}

    /**
     * Strip {@code table.} prefixes from column references so they resolve
     * against the flat schema of {@code __mesh_partial__}. Non-qualified cols
     * pass through unchanged.
     */
    private static List<String> stripQualifiers(List<String> cols) {
        List<String> out = new ArrayList<>(cols.size());
        for (String c : cols) {
            int dot = c.lastIndexOf('.');
            out.add(dot >= 0 ? c.substring(dot + 1) : c);
        }
        return out;
    }

    /**
     * Rewrite each aggregate reference in HAVING with its combine expression.
     * String substitution is fragile in general but sufficient for MVP scope
     * — an aggregate as written in HAVING must match an aggregate in the SELECT
     * list byte-for-byte (whitespace-normalized). If it doesn't, HAVING will
     * fail to compile at the agent (with a real Calcite error).
     */
    private static String rewriteHaving(String having, Map<String, String> aggToCombine) {
        String out = having;
        for (var e : aggToCombine.entrySet()) {
            // Escape regex metachars in the aggregate text. Use word-boundary-safe replace.
            out = out.replace(e.getKey(), e.getValue());
        }
        return out;
    }

    // -- small helpers -------------------------------------------------------

    private record CombineDef(String expr, String alias) {}

    private static java.util.Optional<String> extractWhere(String sql) {
        Matcher m = WHERE_CLAUSE.matcher(sql);
        return m.find() ? java.util.Optional.of(m.group(1).trim()) : java.util.Optional.empty();
    }

    private static java.util.Optional<String> extractJoinChain(String sql) {
        Matcher m = JOIN_CHAIN.matcher(sql);
        return m.find() ? java.util.Optional.of(m.group(1).trim()) : java.util.Optional.empty();
    }

    /**
     * Phase 4b.2.1 — WHERE pushdown to per-side scans. Given the query SQL
     * and one side's table name, return the sub-expression that can be
     * safely evaluated at that side's scan task before shuffle.
     *
     * <p>MVP semantics — safe subset:</p>
     * <ul>
     *   <li>Split the WHERE on top-level {@code AND} (paren-depth = 0)</li>
     *   <li>Keep conjuncts that reference qualifiers exclusively on the
     *       target side (via the {@code table.col} regex)</li>
     *   <li>If the WHERE contains a top-level {@code OR}, we can't split
     *       it into per-side pieces safely — return empty</li>
     *   <li>Predicates with no qualifiers at all (e.g. {@code 1 = 1})
     *       aren't pushed — they'd be applied redundantly on both sides
     *       and burn CPU with no filtering value</li>
     * </ul>
     *
     * <p>The output is the conjuncts joined by {@code AND}, or empty if
     * nothing pushdown-able for this side. The combine SQL still contains
     * the full WHERE — jvssql re-applies redundantly, correct but wasteful
     * at the CPU cost of a second predicate eval per surviving row. Acceptable
     * trade for the bandwidth win.</p>
     */
    public static java.util.Optional<String> computeSidePushdown(String sql, String tableName) {
        Matcher m = WHERE_CLAUSE.matcher(sql);
        if (!m.find()) return java.util.Optional.empty();
        String where = m.group(1).trim();
        // Bail on top-level OR — can't cleanly attribute per side.
        if (hasTopLevelOr(where)) return java.util.Optional.empty();
        List<String> keep = new ArrayList<>();
        for (String conj : splitTopLevelAnd(where)) {
            if (referencesOnlyTable(conj, tableName)) keep.add(conj);
        }
        if (keep.isEmpty()) return java.util.Optional.empty();
        return java.util.Optional.of(String.join(" AND ", keep));
    }

    /** True if the string contains a top-level (paren-depth 0) word "OR". */
    static boolean hasTopLevelOr(String s) {
        int depth = 0;
        Matcher m = OR_TOKEN.matcher(s);
        while (m.find()) {
            int at = m.start();
            depth = 0;
            for (int i = 0; i < at; i++) {
                char c = s.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') depth--;
            }
            if (depth == 0) return true;
        }
        return false;
    }

    /** Split on top-level (paren-depth 0) AND. Case-insensitive. */
    static List<String> splitTopLevelAnd(String s) {
        List<String> out = new ArrayList<>();
        Matcher m = AND_TOKEN.matcher(s);
        int depth = 0, start = 0;
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '(') { depth++; i++; continue; }
            if (c == ')') { depth--; i++; continue; }
            if (depth == 0 && m.find(i) && m.start() == i) {
                String tok = s.substring(start, i).trim();
                if (!tok.isEmpty()) out.add(tok);
                i = m.end();
                start = i;
                continue;
            }
            i++;
        }
        String tail = s.substring(start).trim();
        if (!tail.isEmpty()) out.add(tail);
        return out;
    }

    /**
     * True iff every qualified column reference in {@code predicate} uses
     * the given {@code tableName} as its qualifier. A predicate with NO
     * qualified refs (e.g. {@code col = 1} or {@code 1 = 1}) returns false —
     * we only push down predicates that clearly belong to one side, to avoid
     * ambiguity and to avoid pushing constant predicates that'd be applied
     * uselessly per row.
     */
    static boolean referencesOnlyTable(String predicate, String tableName) {
        Matcher m = QUALIFIED_COL.matcher(predicate);
        boolean sawQualifier = false;
        while (m.find()) {
            if (!m.group(1).equalsIgnoreCase(tableName)) return false;
            sawQualifier = true;
        }
        return sawQualifier;
    }

    private static final Pattern AND_TOKEN =
            Pattern.compile("\\bAND\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern OR_TOKEN =
            Pattern.compile("\\bOR\\b", Pattern.CASE_INSENSITIVE);
    /** Captures {@code qualifier} in {@code qualifier.column}. Matches column
     *  refs of the form {@code alias.field} — not string literals or method
     *  calls. */
    private static final Pattern QUALIFIED_COL =
            Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\.[A-Za-z_][A-Za-z0-9_]*\\b");

    /**
     * Parses the group-by list respecting parentheses so function-call cols
     * like {@code WIN_START(event_time, 60000)} come through as one token.
     * Phase 6d: needed for windowed aggregation over streaming sources.
     */
    private static List<String> parseGroupCols(String groupByRaw) {
        return splitTopLevel(groupByRaw);
    }

    /** True iff {@code s} looks like a bare column reference — no parens, no whitespace, dots allowed for qualifiers. */
    static boolean isSimpleColumnRef(String s) {
        return s != null && s.matches("[A-Za-z_][A-Za-z0-9_.]*");
    }

    /**
     * Split a SELECT list on commas, respecting parentheses so
     * {@code x, foo(a, b), y} yields {@code [x, foo(a, b), y]}.
     * MVP scope: doesn't handle string-literal commas.
     */
    static List<String> splitTopLevel(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                String tok = s.substring(start, i).trim();
                if (!tok.isEmpty()) out.add(tok);
                start = i + 1;
            }
        }
        String tail = s.substring(start).trim();
        if (!tail.isEmpty()) out.add(tail);
        return out;
    }

    private static void appendJoin(StringBuilder sb, List<String> parts) {
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(parts.get(i));
        }
    }

    /**
     * Captures the join kind (group 1: LEFT/RIGHT/FULL/INNER or null=INNER),
     * table name (group 2), and both qualified equijoin columns (groups 3–6).
     * Handles: {@code JOIN}, {@code INNER JOIN}, {@code LEFT [OUTER] JOIN},
     * {@code RIGHT [OUTER] JOIN}, {@code FULL [OUTER] JOIN}.
     */
    private static final Pattern JOIN_ON =
            Pattern.compile("\\b(?:(LEFT|RIGHT|FULL)(?:\\s+OUTER)?\\s+|(INNER)\\s+)?JOIN\\s+"
                    + "([A-Za-z_][A-Za-z0-9_]*)\\s+ON\\s+"
                    + "([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)"
                    + "\\s*=\\s*"
                    + "([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)",
                    Pattern.CASE_INSENSITIVE);

    /**
     * Try to plan the SQL as a phase-4b shuffle-hash join. Returns present
     * only if the SQL is:
     *
     * <ul>
     *   <li>{@code SELECT ... FROM leftT JOIN rightT ON leftT.k1 = rightT.k2}</li>
     *   <li>Both {@code leftT} and {@code rightT} are registered as distributed
     *       tables (i.e. present in {@code distributedTables})</li>
     *   <li>No GROUP BY, no aggregates in the SELECT list, no HAVING, no WHERE
     *       (MVP scope; these compose in phase 4b.1)</li>
     * </ul>
     *
     * <p>Anything more elaborate returns empty and falls through to the
     * normal planner — which will reject fact-x-fact JOINs via
     * {@link #validateJoins} until phase 4b.1 lands.</p>
     */
    private static java.util.Optional<Plan> tryPlanShuffleJoin(
            String sql, java.util.Set<String> distributedTables) {
        Matcher joinM = JOIN_ON.matcher(sql);
        if (!joinM.find()) return java.util.Optional.empty();
        Matcher from = FROM_TABLE.matcher(sql);
        if (!from.find()) return java.util.Optional.empty();
        String outerKw = joinM.group(1);      // LEFT / RIGHT / FULL / null
        String rightFromClause = joinM.group(3);
        String qualA = joinM.group(4);
        String colA  = joinM.group(5);
        String qualB = joinM.group(6);
        String colB  = joinM.group(7);
        String leftFromClause = from.group(1);
        com.hitorro.mesh.CombineSpec.JoinKind joinKind = outerKw == null
                ? com.hitorro.mesh.CombineSpec.JoinKind.INNER
                : com.hitorro.mesh.CombineSpec.JoinKind.valueOf(outerKw.toUpperCase());

        // Both sides must be distributed. Broadcast joins fall through.
        if (!distributedTables.contains(leftFromClause)) return java.util.Optional.empty();
        if (!distributedTables.contains(rightFromClause)) return java.util.Optional.empty();

        // Match up which qualifier is which side.
        String leftKey, rightKey;
        if (qualA.equalsIgnoreCase(leftFromClause) && qualB.equalsIgnoreCase(rightFromClause)) {
            leftKey = colA; rightKey = colB;
        } else if (qualA.equalsIgnoreCase(rightFromClause) && qualB.equalsIgnoreCase(leftFromClause)) {
            leftKey = colB; rightKey = colA;
        } else {
            // Qualifiers don't line up with the FROM/JOIN tables — bail.
            return java.util.Optional.empty();
        }

        // Phase 4b.1: WHERE composes trivially — the combine SQL is the ORIGINAL
        // query verbatim, so per-bucket combines just apply the WHERE against
        // their local rows. Correctness doesn't need any planner changes.
        // (Optimization: pushing single-side WHERE predicates down to the
        // per-side scan tasks would reduce shuffle bandwidth, deferred to a
        // follow-up.)
        //
        // Phase 4b.2: GROUP BY + aggregates on top of shuffle-join. Delegate
        // to the two-stage rewriter for partial/combine SQL construction,
        // then wrap with the shuffle-join metadata the dispatcher needs.
        // Handled below — falls through this early-return branch.
        Matcher gbHere = HAS_GROUP_BY.matcher(sql);
        boolean hasGroupBy = gbHere.find();
        boolean hasAgg = AGG_CALL.matcher(sql).find();
        if (hasGroupBy || hasAgg) {
            // planTwoStage handles empty groupCols (global aggregate case:
            // SELECT COUNT(*) FROM leftT JOIN rightT — one row output).
            List<String> groupCols = hasGroupBy
                    ? parseGroupCols(gbHere.group(1).trim())
                    : List.of();
            Matcher havingM = HAVING_CLAUSE.matcher(sql);
            String having = havingM.find() ? havingM.group(1).trim() : null;
            TwoStagePlan inner = planTwoStage(sql, leftFromClause, groupCols, having);
            return java.util.Optional.of(new ShuffleJoinAggregatePlan(
                    leftFromClause, leftFromClause, rightFromClause,
                    leftKey, rightKey, joinKind,
                    inner.partialSql(), inner.combineSql(),
                    inner.groupColumns(), inner.partialColumns()));
        }

        // All four join kinds now supported by jvssql (INNER / LEFT / RIGHT / FULL).
        return java.util.Optional.of(new ShuffleJoinPlan(
                leftFromClause, leftFromClause, rightFromClause,
                leftKey, rightKey, sql, joinKind));
    }

    /**
     * If the SQL ends with {@code LIMIT N}, return the numeric limit.
     * Otherwise empty. Only recognizes the trailing form — a mid-query
     * subquery LIMIT is not extracted.
     */
    public static java.util.OptionalLong parseLimit(String sql) {
        Matcher m = LIMIT_CLAUSE.matcher(sql);
        if (!m.find()) return java.util.OptionalLong.empty();
        return java.util.OptionalLong.of(Long.parseLong(m.group(1)));
    }

    /** Return {@code sql} with any trailing {@code LIMIT N} clause removed. */
    public static String stripLimit(String sql) {
        Matcher m = LIMIT_CLAUSE.matcher(sql);
        return m.find() ? sql.substring(0, m.start()).stripTrailing() : sql;
    }

    /** {@code col} to sort on + {@link Direction}. Phase 5b. */
    public record OrderKey(String column, Direction direction) {
        public enum Direction { ASC, DESC }
    }

    /**
     * Parse an {@code ORDER BY} clause into a list of {@link OrderKey}s in
     * left-to-right order. Returns an empty list if no ORDER BY is present.
     *
     * <p>MVP scope: only column references (possibly qualified like
     * {@code docs.id}); no expressions, function calls, or NULLS FIRST/LAST
     * hints. Direction is ASC by default; DESC when a token ends in DESC.</p>
     */
    public static List<OrderKey> parseOrderBy(String sql) {
        Matcher m = ORDER_BY_CLAUSE.matcher(sql);
        if (!m.find()) return List.of();
        List<OrderKey> out = new ArrayList<>();
        for (String tok : splitTopLevel(m.group(1).trim())) {
            String[] parts = tok.trim().split("\\s+");
            String col = parts[0];
            OrderKey.Direction dir = OrderKey.Direction.ASC;
            if (parts.length > 1 && "DESC".equalsIgnoreCase(parts[parts.length - 1])) {
                dir = OrderKey.Direction.DESC;
            }
            // Strip table qualifier — the row we compare has flat column names.
            int dot = col.lastIndexOf('.');
            if (dot >= 0) col = col.substring(dot + 1);
            out.add(new OrderKey(col, dir));
        }
        return out;
    }

    /** Return {@code sql} with any {@code ORDER BY} clause removed (but leave LIMIT if present). */
    public static String stripOrderBy(String sql) {
        Matcher m = ORDER_BY_CLAUSE.matcher(sql);
        if (!m.find()) return sql;
        String before = sql.substring(0, m.start()).stripTrailing();
        String after = sql.substring(m.end()).stripLeading();
        return after.isEmpty() ? before : before + " " + after;
    }

    /**
     * Every {@code JOIN &lt;t&gt;} in the SQL must reference a table that is
     * either the distributed source (which will already be scanned locally at
     * the agent) or a registered broadcast table (which lives on every agent).
     *
     * <p>Broadcast joins compose transparently with GROUP BY / HAVING / shuffle
     * because the join executes per-partition at each agent — the combine
     * stage sees pre-joined rows and doesn't need to re-touch the broadcast
     * data.</p>
     *
     * <p>Non-broadcast JOINs (fact × fact, two large distributed tables) need
     * a shuffle-hash join across matching key partitions on both sides —
     * that's phase 4b, and this validator rejects those queries with a clear
     * message so nobody's dashboard silently returns per-partition partial
     * joins.</p>
     */
    private static void validateJoins(String sql, java.util.Set<String> broadcastTables) {
        Matcher joinIt = JOIN_TABLE.matcher(sql);
        while (joinIt.find()) {
            String rightTable = joinIt.group(1);
            if (!broadcastTables.contains(rightTable)) {
                throw new IllegalArgumentException(
                        "mesh distribution does not yet support JOIN against table '" + rightTable
                        + "' — only registered broadcast tables can be joined (phase 4a). "
                        + "Non-broadcast JOINs require shuffle-hash join, which is phase 4b. "
                        + "Currently registered broadcast tables: " + broadcastTables);
            }
        }
    }
}
