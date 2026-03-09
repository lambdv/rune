# Rune
a relational algebra query engine
```java
var query = Query.select("users")
    .join(Query.select("subscriptions"))
    .project("username", "email", "plan", "status")
    .rename("username", "user_name")
    .build();

static{
    var schema = new Schema("users", List.of(
        new Attribute("id", Domain.SERIAL),
        new Attribute("username", Domain.STRING),
        new Attribute("email", Domain.STRING)
    ));
    var relation = new Relation(schema, List.of(
        List.of("1", "alice", "alice@example.com"),
        List.of("2", "bob", "bob@example.com"),
        List.of("3", "charlie", "charlie@example.com")
    ));

    var subscriptionsSchema = new Schema("subscriptions", List.of(
        new Attribute("id", Domain.SERIAL),
        new Attribute("user_id", Domain.SERIAL),
        new Attribute("plan", Domain.STRING),
        new Attribute("status", Domain.STRING),
        new Attribute("start_date", Domain.STRING)
    ));
    var subscriptionsRelation = new Relation(subscriptionsSchema, List.of(
        List.of("1", "1", "premium", "active", "2024-01-15"),
        List.of("2", "2", "basic", "active", "2024-02-20"),
        List.of("3", "1", "enterprise", "cancelled", "2023-12-10"),
        List.of("4", "3", "premium", "active", "2024-03-01")
    ));
    PersistancyManager.saveSchema(schema, "data/test2/users.schema.json");
    PersistancyManager.saveRelation(relation, "data/test2/users.csv");
    PersistancyManager.saveSchema(subscriptionsSchema, "data/test2/subscriptions.schema.json");
    PersistancyManager.saveRelation(subscriptionsRelation, "data/test2/subscriptions.csv");
    Query.connectionContext = new ConnectionContext("data/test2/");
}
```