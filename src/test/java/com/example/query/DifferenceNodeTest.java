package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;

public class DifferenceNodeTest {

    @Test
    void testIdenticalSchemasDifference() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var differenceNode = new DifferenceNode(left, right);

        var resultSchema = differenceNode.schema();
        assertEquals("Users_Users", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
        assertEquals(schema.attributes(), resultSchema.attributes());
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
        var differenceNode = new DifferenceNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
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
        var differenceNode = new DifferenceNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
        });
    }

    @Test
    void testSchemaNameConcatenation() {
        var schema = new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL)
        ));
        var left = new SelectNode(schema);
        var right = new SelectNode(schema);
        var differenceNode = new DifferenceNode(left, right);

        assertEquals("Users_Users", differenceNode.schema().name());
    }

    @Test
    void testDifferenceWithProjection() {
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
        var differenceNode = new DifferenceNode(leftProjection, rightProjection);

        var resultSchema = differenceNode.schema();
        assertEquals(2, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("username", Domain.STRING)));
    }

    @Test
    void testDifferenceWithDifferentProjectionsThrowsException() {
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
        var differenceNode = new DifferenceNode(leftProjection, rightProjection);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
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
        var differenceNode = new DifferenceNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
        });
    }

    @Test
    void testLeftOnlyAttributesThrowsException() {
        var leftSchema = new Schema("Left", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING)
        ));
        var rightSchema = new Schema("Right", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING)
        ));
        var left = new SelectNode(leftSchema);
        var right = new SelectNode(rightSchema);
        var differenceNode = new DifferenceNode(left, right);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
        });
    }
}
