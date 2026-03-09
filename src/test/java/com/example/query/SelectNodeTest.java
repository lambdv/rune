package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;

public class SelectNodeTest {
    
    @Test
    void testSchemaStorageAndRetrieval() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        assertEquals(schema, selectNode.schema());
    }
    
    @Test
    void testLeafNodeBehavior() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL)
        ));
        var selectNode = new SelectNode(schema);
        
        assertThrows(UnsupportedOperationException.class, () -> {
            selectNode.child();
        });
    }
    
    @Test
    void testEmptySchema() {
        var schema = new Schema("EmptySchema", List.of());
        var selectNode = new SelectNode(schema);
        assertEquals(schema, selectNode.schema());
        assertEquals(0, selectNode.schema().attributes().size());
    }
    
    @Test
    void testSchemaWithMultipleAttributes() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        assertEquals(6, selectNode.schema().attributes().size());
        assertEquals("Users", selectNode.schema().name());
    }
    
    @Test
    void testSchemaEquality() {
        var schema1 = new Schema("Test", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        var schema2 = new Schema("Test", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        var selectNode1 = new SelectNode(schema1);
        var selectNode2 = new SelectNode(schema2);
        
        assertEquals(selectNode1.schema(), selectNode2.schema());
    }
}
