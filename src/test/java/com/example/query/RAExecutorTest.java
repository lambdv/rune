package com.example.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import com.example.execution.RAExecutor;
import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Relation;
import com.example.model.Schema;
import com.example.storage.PersistancyManager;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RAExecutorTest {
    
    @TempDir
    Path tempDir;
    
    private String getDataPath(String filename) {
        return tempDir.resolve("data/" + filename).toString();
    }
    
    private void setupTestRelation(String schemaName, Schema schema, List<List<String>> rows) {
        var relation = new Relation(schema, rows);
        PersistancyManager.saveSchema(schema, getDataPath(schemaName + ".schema.json"));
        PersistancyManager.saveRelation(relation, getDataPath(schemaName + ".csv"));
    }
    
    @BeforeEach
    void setUp() {
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        setupTestRelation("Users", usersSchema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com"),
            List.of("3", "charlie", "charlie@example.com")
        ));
        
        var profilesSchema = new Schema("Profiles", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("bio", Domain.STRING)
        ));
        setupTestRelation("Profiles", profilesSchema, List.of(
            List.of("1", "25", "Software engineer"),
            List.of("2", "30", "Data scientist"),
            List.of("4", "28", "Designer")
        ));
        
        var adminsSchema = new Schema("Admins", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        setupTestRelation("Admins", adminsSchema, List.of(
            List.of("1", "alice", "alice@example.com"),
            List.of("2", "bob", "bob@example.com")
        ));
    }
    
    @Test
    void testExecuteSelectNode() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        
        var result = executor.execute(selectNode);
        
        assertNotNull(result);
        assertEquals(3, result.rows().size());
        assertEquals(List.of("1", "alice", "alice@example.com"), result.rows().get(0));
        assertEquals(List.of("2", "bob", "bob@example.com"), result.rows().get(1));
        assertEquals(List.of("3", "charlie", "charlie@example.com"), result.rows().get(2));
    }
    
    @Test
    void testExecuteProjectionNode() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING)
            ),
            selectNode
        );
        
        var result = executor.execute(projectionNode);
        
        assertNotNull(result);
        assertEquals(2, result.schema().attributes().size());
        assertEquals(3, result.rows().size());
        assertEquals(List.of("1", "alice"), result.rows().get(0));
        assertEquals(List.of("2", "bob"), result.rows().get(1));
        assertEquals(List.of("3", "charlie"), result.rows().get(2));
    }
    
    @Test
    void testExecuteRenameNode() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of("username", "name", "email", "email_address"),
            selectNode
        );
        
        var result = executor.execute(renameNode);
        
        assertNotNull(result);
        assertEquals(3, result.rows().size());
        assertEquals(List.of("1", "alice", "alice@example.com"), result.rows().get(0));
        assertEquals("name", result.schema().attributes().get(1).name());
        assertEquals("email_address", result.schema().attributes().get(2).name());
    }
    
    @Test
    void testExecuteUnionNode() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var adminsSchema = new Schema("Admins", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var usersNode = new SelectNode(usersSchema);
        var adminsNode = new SelectNode(adminsSchema);
        var unionNode = new UnionNode(usersNode, adminsNode);
        
        var result = executor.execute(unionNode);
        
        assertNotNull(result);
        assertEquals(3, result.rows().size());
        var rows = result.rows().stream()
            .map(row -> row.get(1))
            .collect(Collectors.toSet());
        assertEquals(Set.of("alice", "bob", "charlie"), rows);
    }
    
    @Test
    void testExecuteDifferenceNode() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var adminsSchema = new Schema("Admins", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var usersNode = new SelectNode(usersSchema);
        var adminsNode = new SelectNode(adminsSchema);
        var differenceNode = new DifferenceNode(usersNode, adminsNode);
        
        var result = executor.execute(differenceNode);
        
        assertNotNull(result);
        assertEquals(1, result.rows().size());
        assertEquals(List.of("3", "charlie", "charlie@example.com"), result.rows().get(0));
    }
    
    @Test
    void testExecuteNaturalJoin() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var profilesSchema = new Schema("Profiles", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("bio", Domain.STRING)
        ));
        var usersNode = new SelectNode(usersSchema);
        var profilesNode = new SelectNode(profilesSchema);
        var joinNode = new NatrualJoin(usersNode, profilesNode);
        
        var result = executor.execute(joinNode);
        
        assertNotNull(result);
        assertEquals(2, result.rows().size());
        assertEquals(5, result.schema().attributes().size());
        
        var firstRow = result.rows().get(0);
        assertEquals("1", firstRow.get(0));
        assertTrue(firstRow.contains("alice"));
        assertTrue(firstRow.contains("25"));
    }
    
    @Test
    void testExecuteComplexQuery() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var adminsSchema = new Schema("Admins", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        
        var usersNode = new SelectNode(usersSchema);
        var adminsNode = new SelectNode(adminsSchema);
        var usersProjection = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING)
            ),
            usersNode
        );
        var adminsProjection = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING)
            ),
            adminsNode
        );
        var unionNode = new UnionNode(usersProjection, adminsProjection);
        
        var result = executor.execute(unionNode);
        
        assertNotNull(result);
        assertEquals(2, result.schema().attributes().size());
        assertEquals(3, result.rows().size());
        var usernames = result.rows().stream()
            .map(row -> row.get(1))
            .collect(Collectors.toSet());
        assertEquals(Set.of("alice", "bob", "charlie"), usernames);
    }
    
    @Test
    void testExecuteNestedOperations() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var usersSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        
        var selectNode = new SelectNode(usersSchema);
        var renameNode = new RenameNode(
            Map.of("username", "name"),
            selectNode
        );
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("name", Domain.STRING)
            ),
            renameNode
        );
        
        var result = executor.execute(projectionNode);
        
        assertNotNull(result);
        assertEquals(2, result.schema().attributes().size());
        assertEquals("name", result.schema().attributes().get(1).name());
        assertEquals(3, result.rows().size());
        assertEquals(List.of("1", "alice"), result.rows().get(0));
    }
    
    @Test
    void testExecuteEmptyRelation() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var emptySchema = new Schema("Empty", List.of(
            new Attribute("id", Domain.SERIAL)
        ));
        setupTestRelation("Empty", emptySchema, List.of());
        var selectNode = new SelectNode(emptySchema);
        
        var result = executor.execute(selectNode);
        
        assertNotNull(result);
        assertEquals(0, result.rows().size());
    }
    
    @Test
    void testExecuteProjectionOnEmptyRelation() {
        var executor = new RAExecutor(tempDir.resolve("data").toString());
        var emptySchema = new Schema("Empty", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        setupTestRelation("Empty", emptySchema, List.of());
        var selectNode = new SelectNode(emptySchema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL)),
            selectNode
        );
        
        var result = executor.execute(projectionNode);
        
        assertNotNull(result);
        assertEquals(0, result.rows().size());
        assertEquals(1, result.schema().attributes().size());
    }
}
