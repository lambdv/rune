package com.example.query;

import com.example.model.Attribute;
import com.example.model.Schema;

import java.util.Map;

public record RenameNode(Map<String, String> renames, RANode child) implements UnaryOpNode {
    @Override
    public Schema schema() {
        var mappedAttributes = child().schema().attributes().stream()
                .map(attr -> new Attribute(renames.getOrDefault(attr.name(), attr.name()), attr.domain()))
                .toList();
        return new Schema(child().schema().name(), mappedAttributes);
    }
}
