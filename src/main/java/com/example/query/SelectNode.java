package com.example.query;

import com.example.model.Schema;

public record SelectNode(Schema schema) implements UnaryOpNode {
    @Override
    public RANode child() {
        throw new UnsupportedOperationException("SelectNode is a leaf node");
    }
}
