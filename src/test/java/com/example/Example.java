package com.example;

import com.example.builder.Query;
import com.example.execution.RAExecutor;
import com.example.builder.ConnectionContext;
import com.example.model.Schema;
import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Relation;
import com.example.storage.PersistancyManager;

import java.util.List;

public class Example {
    public static void main(String[] args) {
        var schema = new Schema("users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        PersistancyManager.saveSchema(schema, "data/test2/users.schema.json");
        var relation = new Relation(schema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com"),
            List.of("3", "charlie", "charlie@example.com")
        ));
        PersistancyManager.saveRelation(relation, "data/test2/users.csv");

        var subscriptionsSchema = new Schema("subscriptions", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("user_id", Domain.SERIAL),
            new Attribute("plan", Domain.STRING),
            new Attribute("status", Domain.STRING),
            new Attribute("start_date", Domain.STRING)
        ));
        PersistancyManager.saveSchema(subscriptionsSchema, "data/test2/subscriptions.schema.json");
        var subscriptionsRelation = new Relation(subscriptionsSchema, List.of(
            List.of("1", "1", "premium", "active", "2024-01-15"),
            List.of("2", "2", "basic", "active", "2024-02-20"),
            List.of("3", "1", "enterprise", "cancelled", "2023-12-10"),
            List.of("4", "3", "premium", "active", "2024-03-01")
        ));
        PersistancyManager.saveRelation(subscriptionsRelation, "data/test2/subscriptions.csv");

        Query.connectionContext = new ConnectionContext("data/test2/");
        var executor = new RAExecutor("data/test2/");
        var query = Query.select("users")
            .join(Query.select("subscriptions"))
            .project("username", "email", "plan", "status")
            .rename("username", "user_name")
            .build();
        
        var result = executor.execute(query);
        System.out.println("Query result:");
        System.out.println("Schema: " + result.schema());
        System.out.println("Rows: " + result.rows().size());
        result.rows().forEach(row -> System.out.println(row));
    }
}
