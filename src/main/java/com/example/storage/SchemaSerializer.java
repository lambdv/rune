package com.example.storage;

import com.example.model.Schema;

public interface SchemaSerializer<T> {
    T serialize(Schema schema);
    Schema deserialize(T serialized);
}
