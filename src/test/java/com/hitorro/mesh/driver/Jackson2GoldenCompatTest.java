/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Pins the driver's JSON-dependent decisions to 3.0.2 on Jackson 2: {@link ShufflePlacement#bucketFor}
 * (same cases as hitorro-mesh-agent's golden test, whose bucket values are identical, so agents and
 * driver keep agreeing), the ORDER BY merge comparison for every pair of 21 value kinds, and the
 * partial-aggregate type synthesized from a row. {@code jackson2-golden.txt} was produced by running
 * the same program against the 3.0.2 classes on Jackson 2.18.
 */
class Jackson2GoldenCompatTest {
  // Same rows, key lists and bucket counts as hitorro-mesh-agent's golden test: the two must agree.
  static final String[] ROWS = {
    "{\"k\":\"a\",\"n\":1,\"d\":2.5,\"b\":true}",
    "{\"k\":\"a\",\"n\":12345678901,\"d\":1.10,\"b\":false}",
    "{\"k\":\"\u00e9\u20ac\ud83d\ude00\",\"n\":12345678901234567890,\"d\":-0.0,\"b\":null}",
    "{\"k\":null,\"n\":\"7\",\"d\":1e30}",
    "{\"n\":-3,\"d\":3.7,\"b\":\"x\"}",
    "{\"k\":{\"x\":1},\"n\":[1,2],\"d\":{},\"b\":[]}",
    "{\"k\":\"\",\"n\":0,\"d\":0.0,\"b\":1}",
    "{\"k\":\"__NULL__\",\"n\":1.0,\"d\":1E+2,\"b\":\"\\u0000\"}"
  };
  static final List<List<String>> KEYS = List.of(List.of("k"), List.of("n"), List.of("d"), List.of("b"), List.of("k", "n"), List.of("n", "k", "d", "b"), List.of("missing"), List.of());
  static final int[] BUCKETS = {1, 4, 7, 16, 1000};
  static final String[] VALUES = {"1", "2", "1.5", "-0.0", "0", "12345678901234567890", "1e30", "\"a\"", "\"B\"", "\"10\"", "\"9\"", "true", "false", "null", "{}", "{\"x\":1}", "[1]", "[]", "\"\""};

  static String generate() throws Exception {
    tools.jackson.databind.ObjectMapper m = com.hitorro.util.json.JsonMappers.json();
    StringBuilder out = new StringBuilder();
    for (int r = 0; r < ROWS.length; r++) {
      tools.jackson.databind.JsonNode row = m.readTree(ROWS[r]);
      for (int k = 0; k < KEYS.size(); k++)
        for (int b : BUCKETS)
          out.append("bucket.row").append(r).append(".keys").append(k).append(".b").append(b).append('=')
             .append(ShufflePlacement.bucketFor(row, KEYS.get(k), b)).append('\n');
    }
    out.append("bucket.nullrow=").append(ShufflePlacement.bucketFor(null, List.of("k", "n"), 16)).append('\n');
    Class<?> oc = Class.forName("com.hitorro.mesh.driver.QueryDispatcher$OrderComparator");
    Method cmp = oc.getDeclaredMethod("compareOne", tools.jackson.databind.JsonNode.class, tools.jackson.databind.JsonNode.class);
    cmp.setAccessible(true);
    List<tools.jackson.databind.JsonNode> vals = new ArrayList<>();
    for (String v : VALUES) vals.add(m.readTree(v));
    vals.add(null);
    vals.add(tools.jackson.databind.node.MissingNode.getInstance());
    for (int i = 0; i < vals.size(); i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < vals.size(); j++) {
        String c;
        try { c = String.valueOf(Integer.signum((Integer) cmp.invoke(null, vals.get(i), vals.get(j)))); }
        catch (java.lang.reflect.InvocationTargetException e) { c = "!" + e.getCause().getClass().getSimpleName(); }
        sb.append(c).append(',');
      }
      out.append("order.compare.").append(i).append('=').append(sb).append('\n');
    }
    Method st = QueryDispatcher.class.getDeclaredMethod("synthesizePartialTypeFromRow", tools.jackson.databind.JsonNode.class);
    st.setAccessible(true);
    for (int r = 0; r < ROWS.length; r++) {
      com.hitorro.jsontypesystem.Type t = (com.hitorro.jsontypesystem.Type) st.invoke(null, m.readTree(ROWS[r]));
      StringBuilder b = new StringBuilder(t.getName()).append(':');
      for (String name : new String[]{"k", "n", "d", "b", "missing"}) {
        com.hitorro.jsontypesystem.Field f = t.getField(name);
        b.append(name).append('=').append(f == null ? "-" : String.valueOf(f.getMetaNode().get("type"))).append(';');
      }
      out.append("partialType.row").append(r).append('=').append(b).append('\n');
    }
    return out.toString();
  }

  @Test
  void matchesJackson2() throws Exception {
    Map<String, String> golden = new LinkedHashMap<>();
    try (InputStream in = getClass().getResourceAsStream("jackson2-golden.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    Map<String, String> actual = new LinkedHashMap<>();
    for (String line : generate().split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    List<String> diffs = new ArrayList<>();
    for (String k : golden.keySet())
      if (!Objects.equals(golden.get(k), actual.get(k))) diffs.add(k + ": jackson2=" + golden.get(k) + " now=" + actual.get(k));
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden)");
    org.junit.jupiter.api.Assertions.assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    org.junit.jupiter.api.Assertions.assertEquals(350, golden.size());
  }
}
