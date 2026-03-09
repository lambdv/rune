package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;
import java.util.Map;

public class RenameNodeTest {
    
    private Schema createTestSchema() {
        return new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING)
        ));
    }
    
    @Test
    void testSingleAttributeRename() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of("username", "user_name"),
            child
        );
        
        var expectedSchema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("user_name", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING)
        ));
        
        assertEquals(expectedSchema, renameNode.schema());
    }
    
    @Test
    void testMultipleAttributeRenames() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of(
                "username", "user_name",
                "email", "email_address",
                "password", "pwd"
            ),
            child
        );
        
        var expectedSchema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("user_name", Domain.STRING),
            new Attribute("email_address", Domain.STRING),
            new Attribute("pwd", Domain.STRING)
        ));
        
        assertEquals(expectedSchema, renameNode.schema());
    }
    
    @Test
    void testEmptyRenameMap() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(Map.of(), child);
        
        assertEquals(schema, renameNode.schema());
    }
    
    @Test
    void testRenameNonExistentAttribute() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of("nonexistent", "new_name"),
            child
        );
        
        var attributes = renameNode.schema().attributes();
        assertEquals(4, attributes.size());
        assertEquals("id", attributes.get(0).name());
        assertEquals("username", attributes.get(1).name());
        assertEquals("email", attributes.get(2).name());
        assertEquals("password", attributes.get(3).name());
    }
    
    @Test
    void testPreservesAttributeDomains() {
        var schema = new Schema("Test", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("active", Domain.BOOLEAN),
            new Attribute("name", Domain.STRING)
        ));
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of(
                "id", "identifier",
                "age", "years",
                "active", "is_active"
            ),
            child
        );
        
        var attributes = renameNode.schema().attributes();
        assertEquals(Domain.SERIAL, attributes.get(0).domain());
        assertEquals(Domain.NUMBER, attributes.get(1).domain());
        assertEquals(Domain.BOOLEAN, attributes.get(2).domain());
        assertEquals(Domain.STRING, attributes.get(3).domain());
    }
    
    @Test
    void testPreservesAttributeOrder() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of(
                "email", "email_address",
                "id", "identifier"
            ),
            child
        );
        
        var attributes = renameNode.schema().attributes();
        assertEquals("identifier", attributes.get(0).name());
        assertEquals("username", attributes.get(1).name());
        assertEquals("email_address", attributes.get(2).name());
        assertEquals("password", attributes.get(3).name());
    }
    
    @Test
    void testRenameAllAttributes() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of(
                "id", "identifier",
                "username", "user_name",
                "email", "email_address",
                "password", "pwd"
            ),
            child
        );
        
        var attributes = renameNode.schema().attributes();
        assertEquals(4, attributes.size());
        assertEquals("identifier", attributes.get(0).name());
        assertEquals("user_name", attributes.get(1).name());
        assertEquals("email_address", attributes.get(2).name());
        assertEquals("pwd", attributes.get(3).name());
    }
    
    @Test
    void testSchemaNamePreserved() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of("username", "user_name"),
            child
        );
        
        assertEquals("TestSchema", renameNode.schema().name());
    }
}
