package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;

public class UnionNodeTest {

    @Test
    void testIdenticalSchemasUnion() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var unionNode = new UnionNode(left, right);

        var resultSchema = unionNode.schema();
        assertEquals("Users_Users", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
        assertEquals(schema.attributes(), resultSchema.attributes());
    }

    @Test
    void testUnionCompatibleSchemas() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var unionNode = new UnionNode(left, right);

        var resultSchema = unionNode.schema();
        assertEquals("Left_Right", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
    }

    @Test
    void testDifferentAttributesThrowsException() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("age", Domain.NUMBER)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var unionNode = new UnionNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            unionNode.schema();
        });
    }

    @Test
    void testNoCommonAttributesThrowsException() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("age", Domain.NUMBER),
            new Attribute("email", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var unionNode = new UnionNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            unionNode.schema();
        });
    }

    @Test
    void testSchemaNameConcatenation() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var unionNode = new UnionNode(left, right);

        assertEquals("Users_Users", unionNode.schema().name());
    }

    @Test
    void testUnionWithProjection() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var leftChild = new SelectNode(schema);
        var rightChild = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftChild
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            rightChild
        );
        var unionNode = new UnionNode(leftProjection, rightProjection);

        var resultSchema = unionNode.schema();
        assertEquals(2, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("username", Domain.STRING)));
    }

    @Test
    void testUnionWithDifferentProjectionsThrowsException() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var leftChild = new SelectNode(schema);
        var rightChild = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftChild
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("email", Domain.STRING)),
            rightChild
        );
        var unionNode = new UnionNode(leftProjection, rightProjection);

        assertThrows(IllegalArgumentException.class, () -> {
            unionNode.schema();
        });
    }

    @Test
    void testDifferentAttributeOrderThrowsException() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("username", Domain.STRING),
            new Attribute("id", Domain.SERIAL)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var unionNode = new UnionNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            unionNode.schema();
        });
    }
}
