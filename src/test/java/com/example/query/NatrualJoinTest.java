package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;

public class NatrualJoinTest {
    
    @Test
    void testJoinWithOverlappingAttributes() {
        var leftSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var rightSchema = new Schema("Profiles", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER),
            new Attribute("bio", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var joinNode = new NatrualJoin(left, right);
        
        var resultSchema = joinNode.schema();
        assertEquals("Users_Profiles", resultSchema.name());
        
        var attributes = resultSchema.attributes();
        assertEquals(5, attributes.size());
        assertTrue(attributes.contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("username", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("email", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("age", Domain.NUMBER)));
        assertTrue(attributes.contains(new Attribute("bio", Domain.STRING)));
    }
    
    @Test
    void testJoinWithNoOverlappingAttributes() {
        var leftSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var rightSchema = new Schema("Products", List.of(
            new Attribute("product_id", Domain.SERIAL),
            new Attribute("name", Domain.STRING),
            new Attribute("price", Domain.NUMBER)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var joinNode = new NatrualJoin(left, right);
        
        var resultSchema = joinNode.schema();
        assertEquals("Users_Products", resultSchema.name());
        
        var attributes = resultSchema.attributes();
        assertEquals(5, attributes.size());
        assertTrue(attributes.contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("username", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("product_id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("name", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("price", Domain.NUMBER)));
    }
    
    @Test
    void testJoinWithSubsetOverlappingAttributes() {
        var leftSchema = new Schema("Orders", List.of(
            new Attribute("order_id", Domain.SERIAL),
            new Attribute("user_id", Domain.SERIAL),
            new Attribute("total", Domain.NUMBER)
        ));
        var rightSchema = new Schema("Users", List.of(
            new Attribute("user_id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var joinNode = new NatrualJoin(left, right);
        
        var resultSchema = joinNode.schema();
        assertEquals("Orders_Users", resultSchema.name());
        
        var attributes = resultSchema.attributes();
        assertEquals(5, attributes.size());
        assertTrue(attributes.contains(new Attribute("order_id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("user_id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("total", Domain.NUMBER)));
        assertTrue(attributes.contains(new Attribute("username", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("email", Domain.STRING)));
    }
    
    @Test
    void testSchemaNameConcatenation() {
        var schema = new Schema("Test", List.of(
            new Attribute("id", Domain.SERIAL)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var joinNode = new NatrualJoin(left, right);
        
        assertEquals("Test_Test", joinNode.schema().name());
    }
    
    @Test
    void testJoinDeduplicatesAttributes() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING),
            new Attribute("value", Domain.NUMBER)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING),
            new Attribute("other", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var joinNode = new NatrualJoin(left, right);
        
        var attributes = joinNode.schema().attributes();
        assertEquals(4, attributes.size());
        assertTrue(attributes.contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("name", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("value", Domain.NUMBER)));
        assertTrue(attributes.contains(new Attribute("other", Domain.STRING)));
    }
    
    @Test
    void testJoinWithProjection() {
        var leftSchema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var rightSchema = new Schema("Profiles", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("age", Domain.NUMBER)
        ));
        var leftChild = new SelectNode(leftSchema);
        var rightChild = new SelectNode(rightSchema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftChild
        );
        var joinNode = new NatrualJoin(leftProjection, rightChild);
        
        var resultSchema = joinNode.schema();
        var attributes = resultSchema.attributes();
        assertEquals(3, attributes.size());
        assertTrue(attributes.contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(attributes.contains(new Attribute("username", Domain.STRING)));
        assertTrue(attributes.contains(new Attribute("age", Domain.NUMBER)));
    }
    
    @Test
    void testJoinPreservesAttributeOrder() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("a", Domain.STRING),
            new Attribute("b", Domain.STRING),
            new Attribute("c", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("d", Domain.STRING),
            new Attribute("b", Domain.STRING),
            new Attribute("e", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var joinNode = new NatrualJoin(left, right);
        
        var attributes = joinNode.schema().attributes();
        assertEquals("a", attributes.get(0).name());
        assertEquals("b", attributes.get(1).name());
        assertEquals("c", attributes.get(2).name());
        assertEquals("d", attributes.get(3).name());
        assertEquals("e", attributes.get(4).name());
    }
}
