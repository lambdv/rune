package com.example.builder;

import com.example.model.Attribute;
import com.example.model.Schema;
import com.example.query.RANode;
import com.example.query.SelectNode;
import com.example.query.ProjectionNode;
import com.example.query.RenameNode;
import com.example.query.NatrualJoin;
import com.example.query.UnionNode;
import com.example.query.DifferenceNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryBuilder {
    private final RANode node;
    
    public QueryBuilder(Schema schema) {
        this.node = new SelectNode(schema);
    }
    
    private QueryBuilder(RANode node) {
        this.node = node;
    }
    
    public QueryBuilder project(String... attributeNames) {
        var currentSchema = node.schema();
        var attributes = Arrays.stream(attributeNames)
            .map(name -> currentSchema.attributes().stream()
                .filter(attr -> attr.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Attribute '" + name + "' not found in schema. Available attributes: " +
                    currentSchema.attributes().stream()
                        .map(Attribute::name)
                        .collect(Collectors.joining(", "))
                )))
            .collect(Collectors.toList());
        
        return new QueryBuilder(new ProjectionNode(attributes, node));
    }
    
    public QueryBuilder rename(String oldName, String newName) {
        return rename(Map.of(oldName, newName));
    }
    
    public QueryBuilder rename(Map<String, String> renames) {
        var currentSchema = node.schema();
        
        for (var oldName : renames.keySet()) {
            boolean exists = currentSchema.attributes().stream()
                .anyMatch(attr -> attr.name().equals(oldName));
            if (!exists) {
                throw new IllegalArgumentException(
                    "Attribute '" + oldName + "' not found in schema. Available attributes: " +
                    currentSchema.attributes().stream()
                        .map(Attribute::name)
                        .collect(Collectors.joining(", "))
                );
            }
        }
        
        return new QueryBuilder(new RenameNode(renames, node));
    }
    
    public QueryBuilder join(QueryBuilder other) {
        return new QueryBuilder(new NatrualJoin(this.node, other.node));
    }
    
    public QueryBuilder union(QueryBuilder other) {
        return new QueryBuilder(new UnionNode(this.node, other.node));
    }
    
    public QueryBuilder difference(QueryBuilder other) {
        return new QueryBuilder(new DifferenceNode(this.node, other.node));
    }
    
    public RANode build() {
        return node;
    }
}
