/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for phase 4b.2.1 WHERE pushdown helpers. Offline — no cluster,
 * no jvssql compilation, just verifies the string decomposition logic.
 * End-to-end correctness of the pushdown is exercised by the existing
 * {@code ShuffleJoinTest} suite (which now runs with pushdown enabled).
 */
class QueryPlannerPushdownTest {

    // -- splitTopLevelAnd ---------------------------------------------------

    @Test
    void splitTopLevelAnd_singleConjunct_returnsOneToken() {
        assertThat(QueryPlanner.splitTopLevelAnd("a = 1"))
                .containsExactly("a = 1");
    }

    @Test
    void splitTopLevelAnd_multipleConjuncts_returnsAllTokens() {
        assertThat(QueryPlanner.splitTopLevelAnd("a = 1 AND b = 2 AND c = 3"))
                .containsExactly("a = 1", "b = 2", "c = 3");
    }

    @Test
    void splitTopLevelAnd_respectsParens_treatsNestedAndAsSingleToken() {
        // The inner AND is inside parens — the split shouldn't fire there.
        assertThat(QueryPlanner.splitTopLevelAnd("(a = 1 AND b = 2) AND c = 3"))
                .containsExactly("(a = 1 AND b = 2)", "c = 3");
    }

    @Test
    void splitTopLevelAnd_caseInsensitive() {
        assertThat(QueryPlanner.splitTopLevelAnd("a = 1 and b = 2 AND c = 3"))
                .containsExactly("a = 1", "b = 2", "c = 3");
    }

    // -- hasTopLevelOr ------------------------------------------------------

    @Test
    void hasTopLevelOr_bareOr_true() {
        assertThat(QueryPlanner.hasTopLevelOr("a = 1 OR b = 2")).isTrue();
    }

    @Test
    void hasTopLevelOr_parenthesizedOr_false() {
        assertThat(QueryPlanner.hasTopLevelOr("(a = 1 OR b = 2) AND c = 3")).isFalse();
    }

    @Test
    void hasTopLevelOr_noOr_false() {
        assertThat(QueryPlanner.hasTopLevelOr("a = 1 AND b = 2")).isFalse();
    }

    // -- referencesOnlyTable ------------------------------------------------

    @Test
    void referencesOnlyTable_singleQualifier_matchesTarget() {
        assertThat(QueryPlanner.referencesOnlyTable("docs.title = 'X'", "docs")).isTrue();
    }

    @Test
    void referencesOnlyTable_qualifierIsOtherTable_false() {
        assertThat(QueryPlanner.referencesOnlyTable("events.action = 'X'", "docs")).isFalse();
    }

    @Test
    void referencesOnlyTable_multipleRefsAllTarget_true() {
        assertThat(QueryPlanner.referencesOnlyTable("docs.title = docs.subtitle", "docs")).isTrue();
    }

    @Test
    void referencesOnlyTable_mixedTables_false() {
        assertThat(QueryPlanner.referencesOnlyTable("docs.id = events.doc_id", "docs")).isFalse();
    }

    @Test
    void referencesOnlyTable_noQualifiers_false() {
        // Constant / unqualified predicates aren't pushed — could be applied
        // uselessly per row without filtering value.
        assertThat(QueryPlanner.referencesOnlyTable("1 = 1", "docs")).isFalse();
        assertThat(QueryPlanner.referencesOnlyTable("title = 'X'", "docs")).isFalse();
    }

    // -- computeSidePushdown (end-to-end) -----------------------------------

    @Test
    void computeSidePushdown_singleSidePredicate_pushesToRightSide() {
        Optional<String> pd = QueryPlanner.computeSidePushdown(
                "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id " +
                "WHERE events.action = 'view'", "events");
        assertThat(pd).contains("events.action = 'view'");
    }

    @Test
    void computeSidePushdown_singleSidePredicate_noneForOtherSide() {
        Optional<String> pd = QueryPlanner.computeSidePushdown(
                "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id " +
                "WHERE events.action = 'view'", "docs");
        assertThat(pd).isEmpty();
    }

    @Test
    void computeSidePushdown_bothSidesPredicates_eachSideGetsItsOwn() {
        String sql = "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id " +
                "WHERE docs.title = 'Q' AND events.action = 'view'";
        assertThat(QueryPlanner.computeSidePushdown(sql, "docs"))
                .contains("docs.title = 'Q'");
        assertThat(QueryPlanner.computeSidePushdown(sql, "events"))
                .contains("events.action = 'view'");
    }

    @Test
    void computeSidePushdown_multiSideConjunct_notPushed() {
        // A single conjunct that references both sides can't be pushed —
        // it needs to be evaluated after the join. Verify neither side
        // picks it up.
        String sql = "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id " +
                "WHERE docs.id = events.doc_id AND docs.title = 'Q'";
        // docs.id = events.doc_id refers to both — not pushable
        // docs.title = 'Q' refers to docs only — pushable to docs
        assertThat(QueryPlanner.computeSidePushdown(sql, "docs"))
                .contains("docs.title = 'Q'");
        assertThat(QueryPlanner.computeSidePushdown(sql, "events")).isEmpty();
    }

    @Test
    void computeSidePushdown_topLevelOr_bailsOut() {
        // Top-level OR can't be safely split. Return empty for both sides.
        String sql = "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id " +
                "WHERE docs.title = 'Q' OR events.action = 'view'";
        assertThat(QueryPlanner.computeSidePushdown(sql, "docs")).isEmpty();
        assertThat(QueryPlanner.computeSidePushdown(sql, "events")).isEmpty();
    }

    @Test
    void computeSidePushdown_noWhere_empty() {
        String sql = "SELECT docs.id FROM docs JOIN events ON docs.id = events.doc_id";
        assertThat(QueryPlanner.computeSidePushdown(sql, "docs")).isEmpty();
    }

    @Test
    void computeSidePushdown_stopsAtGroupBy() {
        // WHERE_CLAUSE regex should stop at GROUP BY so it doesn't slurp
        // trailing GROUP BY / ORDER BY clauses into the predicate.
        String sql = "SELECT docs.id, COUNT(*) FROM docs " +
                "JOIN events ON docs.id = events.doc_id " +
                "WHERE docs.title = 'Q' " +
                "GROUP BY docs.id";
        assertThat(QueryPlanner.computeSidePushdown(sql, "docs"))
                .hasValueSatisfying(pd -> {
                    assertThat(pd).contains("docs.title = 'Q'");
                    assertThat(pd).doesNotContainIgnoringCase("GROUP");
                });
    }
}
