package com.example.model;

import java.util.List;

public record Tuple(List<Domain.Value> values) {
    public Domain.Value Get(int i) {
        return values.get(i);
    }
    public Domain.Value Get(String name, Schema schema) {
        int i = schema.attributes().indexOf(schema.attributes().stream()
            .filter(attr -> attr.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Attribute not found: " + name)));
        return Get(i);
    }
}
