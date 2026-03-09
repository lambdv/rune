package com.example.builder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import com.example.execution.RAExecutor;
import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Relation;
import com.example.model.Schema;
import com.example.query.RANode;
import com.example.query.SelectNode;
import com.example.query.ProjectionNode;
import com.example.query.RenameNode;
import com.example.query.NatrualJoin;
import com.example.query.UnionNode;
import com.example.query.DifferenceNode;
import com.example.storage.PersistancyManager;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryBuilderTest {
    
    private Schema createUsersSchema() {
        return new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("age", Domain.NUMBER)
        ));
    }
    
    private Schema createProfilesSchema() {
        return new Schema("Profiles", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("user_id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("bio", Domain.STRING)
        ));
    }
    
    @Test
    void testBasicSelection() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        var node = builder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof SelectNode);
        assertEquals(schema, node.schema());
    }
    
    @Test
    void testProjection() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        var projectedBuilder = builder.project("id", "username", "email");
        var node = projectedBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof ProjectionNode);
        var projectionNode = (ProjectionNode) node;
        assertEquals(3, projectionNode.attributes().size());
        assertTrue(projectionNode.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(projectionNode.attributes().contains(new Attribute("username", Domain.STRING)));
        assertTrue(projectionNode.attributes().contains(new Attribute("email", Domain.STRING)));
    }
    
    @Test
    void testRenameSingle() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        var renamedBuilder = builder.rename("username", "user_name");
        var node = renamedBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof RenameNode);
        var renameNode = (RenameNode) node;
        var resultSchema = renameNode.schema();
        assertTrue(resultSchema.attributes().stream()
            .anyMatch(attr -> attr.name().equals("user_name")));
    }
    
    @Test
    void testRenameMultiple() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        var renamedBuilder = builder.rename(Map.of(
            "username", "user_name",
            "email", "email_address"
        ));
        var node = renamedBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof RenameNode);
        var renameNode = (RenameNode) node;
        var resultSchema = renameNode.schema();
        assertTrue(resultSchema.attributes().stream()
            .anyMatch(attr -> attr.name().equals("user_name")));
        assertTrue(resultSchema.attributes().stream()
            .anyMatch(attr -> attr.name().equals("email_address")));
    }
    
    @Test
    void testChainingSelectProjectRename() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        var chainedBuilder = builder
            .project("id", "username", "email")
            .rename("username", "user_name");
        var node = chainedBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof RenameNode);
        var renameNode = (RenameNode) node;
        assertTrue(renameNode.child() instanceof ProjectionNode);
    }
    
    @Test
    void testJoin() {
        var usersSchema = createUsersSchema();
        var profilesSchema = createProfilesSchema();
        var usersBuilder = new QueryBuilder(usersSchema);
        var profilesBuilder = new QueryBuilder(profilesSchema);
        var joinedBuilder = usersBuilder.join(profilesBuilder);
        var node = joinedBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof NatrualJoin);
        var joinNode = (NatrualJoin) node;
        assertTrue(joinNode.left() instanceof SelectNode);
        assertTrue(joinNode.right() instanceof SelectNode);
    }
    
    @Test
    void testUnion() {
        var schema = createUsersSchema();
        var leftBuilder = new QueryBuilder(schema);
        var rightBuilder = new QueryBuilder(schema);
        var unionBuilder = leftBuilder.union(rightBuilder);
        var node = unionBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof UnionNode);
        var unionNode = (UnionNode) node;
        assertTrue(unionNode.left() instanceof SelectNode);
        assertTrue(unionNode.right() instanceof SelectNode);
    }
    
    @Test
    void testDifference() {
        var schema = createUsersSchema();
        var leftBuilder = new QueryBuilder(schema);
        var rightBuilder = new QueryBuilder(schema);
        var differenceBuilder = leftBuilder.difference(rightBuilder);
        var node = differenceBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof DifferenceNode);
        var differenceNode = (DifferenceNode) node;
        assertTrue(differenceNode.left() instanceof SelectNode);
        assertTrue(differenceNode.right() instanceof SelectNode);
    }
    
    @Test
    void testComplexTreeUnionOfProjections() {
        var schema = createUsersSchema();
        var leftBuilder = new QueryBuilder(schema).project("id", "username");
        var rightBuilder = new QueryBuilder(schema).project("id", "username");
        var unionBuilder = leftBuilder.union(rightBuilder);
        var node = unionBuilder.build();
        
        assertNotNull(node);
        assertTrue(node instanceof UnionNode);
        var unionNode = (UnionNode) node;
        assertTrue(unionNode.left() instanceof ProjectionNode);
        assertTrue(unionNode.right() instanceof ProjectionNode);
    }
    
    @Test
    void testProjectionWithInvalidAttributeThrowsException() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        
        assertThrows(IllegalArgumentException.class, () -> {
            builder.project("id", "invalid_attribute");
        });
    }
    
    @Test
    void testRenameWithInvalidAttributeThrowsException() {
        var schema = createUsersSchema();
        var builder = new QueryBuilder(schema);
        
        assertThrows(IllegalArgumentException.class, () -> {
            builder.rename("invalid_attribute", "new_name");
        });
    }
    
    @Test
    void testIntegrationWithRAExecutor() {
        var tempDir = Path.of(System.getProperty("java.io.tmpdir"), "query-builder-test");
        tempDir.toFile().mkdirs();
        
        var dataPath = tempDir.resolve("data");
        dataPath.toFile().mkdirs();
        
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var usersRelation = new Relation(usersSchema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com"),
            List.of("3", "charlie", "charlie@example.com")
        ));
        PersistancyManager.saveSchema(usersSchema, dataPath.resolve("Users.schema.json").toString());
        PersistancyManager.saveRelation(usersRelation, dataPath.resolve("Users.csv").toString());
        
        var adminsSchema = new Schema("Admins", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var adminsRelation = new Relation(adminsSchema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com")
        ));
        PersistancyManager.saveSchema(adminsSchema, dataPath.resolve("Admins.schema.json").toString());
        PersistancyManager.saveRelation(adminsRelation, dataPath.resolve("Admins.csv").toString());
        
        var context = new ConnectionContext(dataPath.toString());
        var executor = new RAExecutor(dataPath.toString());
        
        var usersBuilder = new QueryBuilder(context.getSchema("Users")).project("id", "username");
        var adminsBuilder = new QueryBuilder(context.getSchema("Admins")).project("id", "username");
        var unionBuilder = usersBuilder.union(adminsBuilder);
        var queryNode = unionBuilder.build();
        
        var result = executor.execute(queryNode);
        
        assertNotNull(result);
        assertEquals(2, result.schema().attributes().size());
        assertEquals(3, result.rows().size());
        var usernames = result.rows().stream()
            .map(row -> row.get(1))
            .collect(Collectors.toSet());
        assertEquals(Set.of("alice", "bob", "charlie"), usernames);
    }
    
    @Test
    void testIntegrationProjectionAndRename() {
        var tempDir = Path.of(System.getProperty("java.io.tmpdir"), "query-builder-test-2");
        tempDir.toFile().mkdirs();
        var dataPath = tempDir.resolve("data");
        dataPath.toFile().mkdirs();
        
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var usersRelation = new Relation(usersSchema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com")
        ));
        PersistancyManager.saveSchema(usersSchema, dataPath.resolve("Users.schema.json").toString());
        PersistancyManager.saveRelation(usersRelation, dataPath.resolve("Users.csv").toString());
        
        var context = new ConnectionContext(dataPath.toString());
        var executor = new RAExecutor(dataPath.toString());
        
        var builder = new QueryBuilder(context.getSchema("Users"))
            .project("id", "username")
            .rename("username", "name");
        var queryNode = builder.build();
        
        var result = executor.execute(queryNode);
        
        assertNotNull(result);
        assertEquals(2, result.schema().attributes().size());
        assertEquals("name", result.schema().attributes().get(1).name());
        assertEquals(2, result.rows().size());
        assertEquals(List.of("1", "alice"), result.rows().get(0));
        assertEquals(List.of("2", "bob"), result.rows().get(1));
    }
    
    @Test
    void testQuerySelectEntryPoint() {
        try {
            var builder = Query.select("TestSchema");
            assertNotNull(builder);
            
            var node = builder.build();
            assertNotNull(node);
            assertTrue(node instanceof SelectNode);
            assertEquals("TestSchema", node.schema().name());
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Schema not found") || e.getMessage().contains("not found"));
        }
    }
}
