package com.example.builder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;

import com.example.model.Schema;
import com.example.storage.PersistancyManager;

public class ConnectionContext {
    private String basePath = "data/test/";
    private List<Schema> schemas = new ArrayList<>();

    public ConnectionContext() {
        this.schemas = loadSchemas();
    }
    
    public ConnectionContext(String basePath) {
        this.basePath = basePath;
        this.schemas = loadSchemas();
    }

    public List<Schema> schemas() {
        return Collections.unmodifiableList(schemas);
    }

    public Boolean isValidSchema(String schemaName) {
        return schemas.stream().anyMatch(schema -> schema.name().equals(schemaName));
    }

    public Schema getSchema(String schemaName) {
        return schemas.stream()
            .filter(schema -> schema.name().equals(schemaName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Schema not found: " + schemaName));
    }

    private List<Schema>loadSchemas() {
        var path = Path.of(basePath);
        try {   
            if (Files.exists(path)) {
                var files = Files.list(path).filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".schema.json")).toList();
                for (var file : files) {
                    var schema = PersistancyManager.loadSchema(file.toString()).get();
                    schemas.add(schema);
                }
            }
        } catch (IOException e) {
            System.err.println("Error getting schemas from path: " + e.getMessage());
        }
        return schemas; 
    }
}
