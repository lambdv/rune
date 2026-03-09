package com.example.storage;

import com.example.model.Relation;

public interface RelationSerializer<T> {
    T serialize(Relation relation);
    Relation deserialize(T serialized);
}
