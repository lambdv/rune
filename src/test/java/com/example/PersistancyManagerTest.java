package com.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.example.core.PersistancyManager;
import com.example.core.Schema;
import com.example.core.Attribute;
import com.example.core.Domain;
import java.util.List;

public class PersistancyManagerTest {
    @Test void testSaveAndLoadSchema() {
        String filename = "data/test/test.schema.json";
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
    
}
