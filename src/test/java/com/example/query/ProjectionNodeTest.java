package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;

public class ProjectionNodeTest {
    
    private Schema createTestSchema() {
        return new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING)
        ));
    }
    
    @Test
    void testValidProjectionWithSubset() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("username", Domain.STRING),
                new Attribute("email", Domain.STRING)
            ),
            child
        );
        
        var expectedSchema = new Schema("TestSchema", List.of(
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        
        assertEquals(expectedSchema, projectionNode.schema());
        assertEquals(2, projectionNode.schema().attributes().size());
    }
    
    @Test
    void testValidProjectionWithSingleAttribute() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("username", Domain.STRING)),
            child
        );
        
        var expectedSchema = new Schema("TestSchema", List.of(
            new Attribute("username", Domain.STRING)
        ));
        
        assertEquals(expectedSchema, projectionNode.schema());
    }
    
    @Test
    void testValidProjectionWithAllAttributes() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING),
                new Attribute("email", Domain.STRING),
                new Attribute("password", Domain.STRING)
            ),
            child
        );
        
        assertEquals(schema, projectionNode.schema());
        assertEquals(4, projectionNode.schema().attributes().size());
    }
    
    @Test
    void testInvalidAttributeThrowsException() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("nonexistent", Domain.STRING)),
            child
        );
        
        assertThrows(IllegalArgumentException.class, () -> {
            projectionNode.schema();
        });
    }
    
    @Test
    void testInvalidAttributeWithValidOnesThrowsException() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("username", Domain.STRING),
                new Attribute("nonexistent", Domain.STRING)
            ),
            child
        );
        
        assertThrows(IllegalArgumentException.class, () -> {
            projectionNode.schema();
        });
    }
    
    @Test
    void testMaintainsAttributeOrder() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("email", Domain.STRING),
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING)
            ),
            child
        );
        
        var attributes = projectionNode.schema().attributes();
        assertEquals("id", attributes.get(0).name());
        assertEquals("username", attributes.get(1).name());
        assertEquals("email", attributes.get(2).name());
    }
    
    @Test
    void testEmptyAttributeList() {
        var schema = createTestSchema();
        var child = new SelectNode(schema);
        
        var projectionNode = new ProjectionNode(List.of(), child);
        assertEquals(0, projectionNode.schema().attributes().size());
        assertEquals("TestSchema", projectionNode.schema().name());
    }
    
    @Test
    void testProjectionPreservesDomain() {
        var schema = new Schema("Test", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("active", Domain.BOOLEAN)
        ));
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("active", Domain.BOOLEAN)
            ),
            child
        );
        
        var attributes = projectionNode.schema().attributes();
        assertEquals(Domain.SERIAL, attributes.get(0).domain());
        assertEquals(Domain.BOOLEAN, attributes.get(1).domain());
    }
}
