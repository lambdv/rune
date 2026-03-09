package com.example.query;

import com.example.model.Schema;

public record UnionNode(RANode left, RANode right) implements BinaryOpNode {
    @Override
    public Schema schema() {
        var leftSchema = left.schema();
        var rightSchema = right.schema();
        var leftAttributes = leftSchema.attributes();
        var rightAttributes = rightSchema.attributes();

        if (!leftAttributes.equals(rightAttributes)) {
            throw new IllegalArgumentException(
                "Union requires union-compatible schemas (same attributes). Left: " +
                leftAttributes + ", Right: " + rightAttributes
            );
        }

        return new Schema(leftSchema.name() + "_" + rightSchema.name(), leftAttributes);
    }
}