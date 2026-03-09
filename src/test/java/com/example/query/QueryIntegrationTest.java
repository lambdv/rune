package com.example.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.model.Attribute;
import com.example.model.Domain;
import com.example.model.Schema;

import java.util.List;
import java.util.Map;

public class QueryIntegrationTest {
    
    private Schema createUsersSchema() {
        return new Schema("Users", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("username", Domain.STRING),
            new Attribute("email", Domain.STRING),
            new Attribute("password", Domain.STRING)
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
    void testSelectProjectionRenamePipeline() {
        var schema = createUsersSchema();
        var selectNode = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING),
                new Attribute("email", Domain.STRING)
            ),
            selectNode
        );
        var renameNode = new RenameNode(
            Map.of(
                "username", "user_name",
                "email", "email_address"
            ),
            projectionNode
        );
        
        var resultSchema = renameNode.schema();
        assertEquals("Users", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
        assertEquals("id", resultSchema.attributes().get(0).name());
        assertEquals("user_name", resultSchema.attributes().get(1).name());
        assertEquals("email_address", resultSchema.attributes().get(2).name());
    }
    
    @Test
    void testUnionOfTwoProjections() {
        var schema = createUsersSchema();
        var leftSelect = new SelectNode(schema);
        var rightSelect = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftSelect
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            rightSelect
        );
        var unionNode = new UnionNode(leftProjection, rightProjection);
        
        var resultSchema = unionNode.schema();
        assertEquals(2, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("username", Domain.STRING)));
    }
    
    @Test
    void testDifferenceWithMatchingProjections() {
        var schema = createUsersSchema();
        var leftSelect = new SelectNode(schema);
        var rightSelect = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftSelect
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            rightSelect
        );
        var differenceNode = new DifferenceNode(leftProjection, rightProjection);

        var resultSchema = differenceNode.schema();
        assertEquals(2, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("username", Domain.STRING)));
    }

    @Test
    void testDifferenceWithDifferentProjectionsThrowsException() {
        var schema = createUsersSchema();
        var leftSelect = new SelectNode(schema);
        var rightSelect = new SelectNode(schema);
        var leftProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            leftSelect
        );
        var rightProjection = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL)),
            rightSelect
        );
        var differenceNode = new DifferenceNode(leftProjection, rightProjection);

        assertThrows(IllegalArgumentException.class, () -> {
            differenceNode.schema();
        });
    }
    
    @Test
    void testNaturalJoinFollowedByProjection() {
        var usersSchema = createUsersSchema();
        var profilesSchema = createProfilesSchema();
        var usersSelect = new SelectNode(usersSchema);
        var profilesSelect = new SelectNode(profilesSchema);
        var joinNode = new NatrualJoin(usersSelect, profilesSelect);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("id", Domain.SERIAL),
                new Attribute("username", Domain.STRING),
                new Attribute("age", Domain.NUMBER)
            ),
            joinNode
        );
        
        var resultSchema = projectionNode.schema();
        assertEquals("Users_Profiles", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("username", Domain.STRING)));
        assertTrue(resultSchema.attributes().contains(new Attribute("age", Domain.NUMBER)));
    }
    
    @Test
    void testProjectionAfterRename() {
        var schema = createUsersSchema();
        var selectNode = new SelectNode(schema);
        var renameNode = new RenameNode(
            Map.of("username", "user_name", "email", "email_address"),
            selectNode
        );
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("user_name", Domain.STRING)),
            renameNode
        );
        
        var resultSchema = projectionNode.schema();
        assertEquals(1, resultSchema.attributes().size());
        assertEquals("user_name", resultSchema.attributes().get(0).name());
    }
    
    @Test
    void testRenameAfterProjection() {
        var schema = createUsersSchema();
        var selectNode = new SelectNode(schema);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("username", Domain.STRING), new Attribute("email", Domain.STRING)),
            selectNode
        );
        var renameNode = new RenameNode(
            Map.of("username", "user_name", "email", "email_address"),
            projectionNode
        );
        
        var resultSchema = renameNode.schema();
        assertEquals(2, resultSchema.attributes().size());
        assertEquals("user_name", resultSchema.attributes().get(0).name());
        assertEquals("email_address", resultSchema.attributes().get(1).name());
    }
    
    @Test
    void testComplexQueryTree() {
        var usersSchema = createUsersSchema();
        var profilesSchema = createProfilesSchema();
        
        var usersSelect1 = new SelectNode(usersSchema);
        var usersSelect2 = new SelectNode(usersSchema);
        var unionNode = new UnionNode(usersSelect1, usersSelect2);
        var projectionNode = new ProjectionNode(
            List.of(new Attribute("id", Domain.SERIAL), new Attribute("username", Domain.STRING)),
            unionNode
        );
        var renameNode = new RenameNode(
            Map.of("username", "user_name"),
            projectionNode
        );
        
        var resultSchema = renameNode.schema();
        assertEquals("Users_Users", resultSchema.name());
        assertEquals(2, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("user_name", Domain.STRING)));
    }
    
    @Test
    void testJoinWithRenameAndProjection() {
        var usersSchema = createUsersSchema();
        var profilesSchema = createProfilesSchema();
        
        var usersSelect = new SelectNode(usersSchema);
        var profilesSelect = new SelectNode(profilesSchema);
        var usersRename = new RenameNode(
            Map.of("id", "user_id"),
            usersSelect
        );
        var joinNode = new NatrualJoin(usersRename, profilesSelect);
        var projectionNode = new ProjectionNode(
            List.of(
                new Attribute("user_id", Domain.SERIAL),
                new Attribute("username", Domain.STRING),
                new Attribute("age", Domain.NUMBER)
            ),
            joinNode
        );
        
        var resultSchema = projectionNode.schema();
        assertEquals("Users_Profiles", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
    }
    
    @Test
    void testNestedBinaryOperations() {
        var schema1 = new Schema("A", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        var schema2 = new Schema("B", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("name", Domain.STRING)
        ));
        var schema3 = new Schema("C", List.of(
            new Attribute("id", Domain.SERIAL),
            new Attribute("value", Domain.NUMBER)
        ));
        
        var select1 = new SelectNode(schema1);
        var select2 = new SelectNode(schema2);
        var select3 = new SelectNode(schema3);
        
        var union = new UnionNode(select1, select2);
        var join = new NatrualJoin(union, select3);
        
        var resultSchema = join.schema();
        assertEquals("A_B_C", resultSchema.name());
        assertEquals(3, resultSchema.attributes().size());
        assertTrue(resultSchema.attributes().contains(new Attribute("id", Domain.SERIAL)));
        assertTrue(resultSchema.attributes().contains(new Attribute("name", Domain.STRING)));
        assertTrue(resultSchema.attributes().contains(new Attribute("value", Domain.NUMBER)));
    }
}
