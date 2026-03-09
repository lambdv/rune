package com.example.builder;

public class Query {
    public static ConnectionContext connectionContext = new ConnectionContext();

    public static QueryBuilder select(String schemaName) {
        if (!connectionContext.isValidSchema(schemaName)) {
            throw new IllegalArgumentException("Schema not found: " + schemaName);
        }
        var schema = connectionContext.getSchema(schemaName);
        return new QueryBuilder(schema);
    }
}
