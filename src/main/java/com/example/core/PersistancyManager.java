package com.example.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import org.json.JSONObject;
import java.util.List;
import java.util.Optional;

public class PersistancyManager {

    /**
     * saving and loading relational database instances
     * @param relation
     * @param path
     */


    // public static void saveRelation(Relation relation, Path path) {
    //     try {
    //         Files.writeString(path, RelationParser.serialize(relation));
    //     } catch (IOException e) {
    //     }
    // }

    // public static Relation loadRelation(Path path) {
    //     try {
    //         var content = Files.readAllLines(path);

    //         return RelationParser.parse(content);
    //     } catch (IOException e) {
    //         System.err.println("Error loading relation: " + e.getMessage());
    //     }
    //     return null;
    // }

    /**
     * saving and loading schemas
     * @param relation
     * @param filename
     */

    public static void saveSchema(Schema schema, String filename) {
        try {
            var path = Path.of(filename);
            Files.createDirectories(path.getParent());
            Files.writeString(path, schema.toJson().toString());
        } 
        catch (IOException e) {
            System.err.println("Error saving schema: " + e.getMessage());
        }
    }

    public static Optional<Schema> loadSchema(String filename) {
        try {
            var content = Files.readString(Path.of(filename));
            return Optional.of(Schema.fromJson(new JSONObject(content)));
        } 
        catch (IOException e) {
            System.err.println("Error loading schema: " + e.getMessage());
        }
        return Optional.empty();
    }
}


// class RelationParser {
//     public static Relation parse(List<String> content) {
//         var schema = content.get(0);

        
//     }

//     public static String serialize(Relation relation) {
//         return new JSONObject(relation).toString();
//     }

//     private static boolean isValidSchema(){return false;}
// }
