/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.driver;

import com.hitorro.jsontypesystem.Type;
import com.hitorro.util.json.JsonMappers;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The type JSON the driver sends to agents (join and combine tasks) is the type's node written as
 * compact JSON, as on 3.x, also when it nests 600 levels deep (Jackson 2's limit was 1000; Jackson
 * 3's {@code JsonNode.toString()} throws past 500).
 */
class TypeToJsonTest {

    static String typeToJson(Type t) throws Exception {
        Method m = QueryDispatcher.class.getDeclaredMethod("typeToJson", Type.class);
        m.setAccessible(true);
        return (String) m.invoke(null, t);
    }

    @Test
    void writesTheTypeNodeAsCompactJson() throws Exception {
        String deep = "[".repeat(600) + "]".repeat(600);
        String json = "{\"name\":\"t\",\"fields\":[{\"name\":\"id\",\"type\":\"core_string\"}],\"note\":\"\\u00e9\",\"deep\":" + deep + "}";
        Type t = new Type();
        t.init(JsonMappers.json().readTree(json));
        assertThat(typeToJson(t)).isEqualTo(JsonMappers.json().writeValueAsString(JsonMappers.json().readTree(json)));
        assertThat(typeToJson(t)).endsWith(",\"deep\":" + deep + "}");
        assertThat(typeToJson(null)).isNull();
    }
}
