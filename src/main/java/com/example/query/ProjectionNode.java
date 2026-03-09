package com.example.query;

import com.example.model.Attribute;
import com.example.model.Schema;

import java.util.List;

public record ProjectionNode(List<Attribute> attributes, RANode child) implements UnaryOpNode {
    @Override
    public Schema schema() {
        var selectedAttributes = child.schema().attributes().stream()
                .filter(attributes::contains)
                .toList();
        if (selectedAttributes.size() < attributes.size()) {
            throw new IllegalArgumentException("Some attributes not found in child schema");
        }
        return new Schema(child.schema().name(), selectedAttributes);
    }
}
