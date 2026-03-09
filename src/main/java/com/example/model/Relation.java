package com.example.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class Relation {
    private Schema schema;
    private List<List<String>> rows;

    public Relation(Schema schema) {
        this.schema = schema;
        this.rows = new ArrayList<>();
    }
    public Relation(Schema schema, List<List<String>> rows) {
        this.schema = schema;
        this.rows = rows;
    }

    public Schema schema() {
        return schema;
    }
    public List<List<String>> rows() {
        return Collections.unmodifiableList(rows);
    }
}
