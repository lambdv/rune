package com.example.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Relation;
import com.example.model.Schema;
import com.example.storage.PersistancyManager;
import com.example.storage.RelationCsvSerializer;

import java.util.List;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class PersistancyManagerTest {
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() throws IOException {
        var testDir = tempDir.resolve("data/test");
        if (Files.exists(testDir)) {
            try (Stream<Path> files = Files.walk(testDir)) {
                files.sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
            }
        }
        Files.createDirectories(testDir);
    }
    
    @Test
    void testSaveAndLoadSchema() {
        String filename = tempDir.resolve("data/test/test.schema.json").toString();
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        ));
        PersistancyManager.saveSchema(schema, filename);
        var loadedSchema = PersistancyManager.loadSchema(filename).get();
        assertEquals(schema.name(), loadedSchema.name());
        assertEquals(schema.attributes(), loadedSchema.attributes());
    }
    
    @Test
    void testSaveRelationToCsv() throws IOException {
        String filename = tempDir.resolve("data/test/users.csv").toString();
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
        
        PersistancyManager.saveRelation(relation, filename);
        
        var loadedContent = Files.readString(Path.of(filename));
        assertTrue(loadedContent.contains("id,username,email"));
        assertTrue(loadedContent.contains("1,alice,alice@example.com"));
        assertTrue(loadedContent.contains("2,bob,bob@example.com"));
        assertTrue(loadedContent.contains("3,charlie,charlie@example.com"));
    }
    
    @Test
    void testLoadRelationFromCsv() {
        String csvContent = "id,username,email\n1,alice,alice@example.com\n2,bob,bob@example.com\n";
        var schema = new Schema("users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        
        var relation = new RelationCsvSerializer(schema).deserialize(csvContent);
        
        assertEquals(2, relation.rows().size());
        assertEquals(List.of("1", "alice", "alice@example.com"), relation.rows().get(0));
        assertEquals(List.of("2", "bob", "bob@example.com"), relation.rows().get(1));
    }
    
    @Test
    void testSaveAndLoadRelationRoundTrip() throws IOException {
        String filename = tempDir.resolve("data/test/roundtrip.csv").toString();
        var schema = new Schema("products", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING),
            new Attribute("price", Domain.NUMBER)
        ));
        var originalRelation = new Relation(schema, List.of(
            List.of("1", "Widget", "29.99"),
            List.of("2", "Gadget", "49.99"),
            List.of("3", "Gizmo", "99.99")
        ));
        
        PersistancyManager.saveRelation(originalRelation, filename);
        
        var loadedRelation = PersistancyManager.loadRelation(schema, filename);
        
        assertEquals(originalRelation.rows().size(), loadedRelation.rows().size());
        assertEquals(originalRelation.rows(), loadedRelation.rows());
    }
    
    @Test
    void testLoadRelationWithEmptyRows() {
        String csvContent = "id,name\n";
        var schema = new Schema("empty", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        
        var relation = new RelationCsvSerializer(schema).deserialize(csvContent);
        
        assertEquals(0, relation.rows().size());
    }
    
}
