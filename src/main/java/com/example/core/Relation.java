package com.example.core;

import java.util.List;

/**
 * Instance of a schema
 */
public class Relation {
    private Schema schema;
    private String name;
    
    public Relation(String name, List<Attribute> attributes) {
        this.name = name;
        this.schema = new Schema(name, attributes);
    }
    
    public String name() {
        return name;
    }
    
    public Schema schema() {
        return schema;
    }
}
