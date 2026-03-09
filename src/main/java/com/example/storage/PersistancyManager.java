package com.example.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import org.json.JSONObject;

import com.example.model.Relation;
import com.example.model.Schema;

import java.util.Optional;

public class PersistancyManager {
    public static void saveRelation(Relation relation, String filename) {
        try {
            var path = Path.of(filename);
            Files.createDirectories(path.getParent());
            Files.writeString(path, new RelationCsvSerializer(relation.schema()).serialize(relation));
        } catch (IOException e) {
            System.err.println("Error saving relation: " + e.getMessage());
        }
    }

    public static Relation loadRelation(Schema schema, String filename) {
        try {
            var content = Files.readString(Path.of(filename));
            return new RelationCsvSerializer(schema).deserialize(content);
        } catch (IOException e) {
            System.err.println("Error loading relation: " + e.getMessage());
            return null;
        }
    }

    public static void saveSchema(Schema schema, String filename) {
        try {
            var path = Path.of(filename);
            Files.createDirectories(path.getParent());
            Files.writeString(path, new SchemaJsonSerializer().serialize(schema).toString());
        } catch (IOException e) {
            System.err.println("Error saving schema: " + e.getMessage());
        }
    }

    public static Optional<Schema> loadSchema(String filename) {
        try {
            var content = Files.readString(Path.of(filename));
            return Optional.of(new SchemaJsonSerializer().deserialize(new JSONObject(content)));
        } catch (IOException e) {
            System.err.println("Error loading schema: " + e.getMessage());
        }
        return Optional.empty();
    }
}
