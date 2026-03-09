package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;
import java.util.Map;

public class RANodeTest {
    @Test
    void testSelectNode() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        assertEquals(selectNode.schema(), schema);
    }

    @Test
    void testProjectionNode() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        ));
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("username", Domain.STRING)), new SelectNode(schema)
        );
        assertEquals(projectionNode.schema(), new Schema("TestSchema", List.of(new Attribute("username", Domain.STRING))));
    }

    @Test
    void testRenameNode() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        ));
        var renameNode = new RenameNode(
            Map.of("username", "user_name"), new SelectNode(schema)
        );
        assertEquals(renameNode.schema(), new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("user_name", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING),
            new Attribute("created_at", Domain.STRING),
            new Attribute("updated_at", Domain.STRING)
        )));
    }
    
    @Test
    void testUnaryOpNodeInterface() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var child = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("username", Domain.STRING)),
            child
        );
        
        assertTrue(projectionNode instanceof UnaryOpNode);
        assertEquals(child, projectionNode.child());
        assertNotNull(projectionNode.schema());
    }
    
    @Test
    void testBinaryOpNodeInterface() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var unionNode = new UnionNode(left, right);
        
        assertTrue(unionNode instanceof BinaryOpNode);
        assertEquals(left, unionNode.left());
        assertEquals(right, unionNode.right());
        assertNotNull(unionNode.schema());
    }
    
    @Test
    void testRANodeSchemaContract() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        
        assertTrue(selectNode instanceof RANode);
        assertNotNull(selectNode.schema());
        assertEquals(schema, selectNode.schema());
    }
    
    @Test
    void testUnaryOpNodeChaining() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var selectNode = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("username", Domain.STRING)),
            selectNode
        );
        var renameNode = new RenameNode(
            Map.of("username", "user_name"),
            projectionNode
        );
        
        assertTrue(selectNode instanceof RANode);
        assertTrue(projectionNode instanceof UnaryOpNode);
        assertTrue(renameNode instanceof UnaryOpNode);
        
        assertEquals(selectNode, projectionNode.child());
        assertEquals(projectionNode, renameNode.child());
    }
    
    @Test
    void testBinaryOpNodeWithUnaryChildren() {
        var schema = new Schema("TestSchema", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var leftSelect = new SelectNode(schema);
        var rightSelect = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL)),
            leftSelect
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL)),
            rightSelect
        );
        var unionNode = new UnionNode(leftProjection, rightProjection);
        
        assertTrue(unionNode instanceof BinaryOpNode);
        assertEquals(leftProjection, unionNode.left());
        assertEquals(rightProjection, unionNode.right());
    }
}
