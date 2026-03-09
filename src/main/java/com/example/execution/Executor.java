package com.example.execution;

import com.example.model.Relation;
import com.example.query.RANode;

public interface Executor {
    Relation execute(RANode node);
}
