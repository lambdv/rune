package com.example.query;

import java.util.function.BiFunction;

import com.example.model.Schema;
import com.example.model.Attribute;
import java.util.ArrayList;

public interface JoinNode extends BinaryOpNode {
    default Schema schema() {
        return joiner().apply(left().schema(), right().schema());
    }
    default BiFunction<Schema, Schema, Schema> joiner() {
        return (left, right) -> {
            var leftAttributes = left.attributes();
            var rightAttributes = right.attributes();
            var attributes = new ArrayList<Attribute>();
            for (var attribute : leftAttributes) {
                attributes.add(attribute);
            }
            for (var attribute : rightAttributes) {
                if (!attributes.contains(attribute)) {
                    attributes.add(attribute);
                }
            }
            return new Schema(left.name() + "_" + right.name(), attributes);
        };
    }    
}