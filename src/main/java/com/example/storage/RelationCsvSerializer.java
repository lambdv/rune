package com.example.storage;

import com.example.model.Relation;
import com.example.model.Schema;

import java.util.ArrayList;
import java.util.List;

public class RelationCsvSerializer implements RelationSerializer<String> {
    private final Schema schema;

    public RelationCsvSerializer(Schema schema) {
        this.schema = schema;
    }

    public String serialize(Relation relation) {
        var csv = "";
        csv += relation.schema().attributes().stream()
            .map(a -> a.name())
            .reduce((a, b) -> a + "," + b)
            .orElse("") + "\n";
        for (var row : relation.rows()) {
            csv += String.join(",", row) + "\n";
        }
        return csv;
    }

    public Relation deserialize(String csv) {
        var lines = csv.split("\n");
        var rows = new ArrayList<List<String>>();
        for (var line : lines) {
            if (line.isEmpty()) continue;
            rows.add(List.of(line.split(",")));
        }
        if (!rows.isEmpty()) {
            rows.remove(0); // drop header row
        }
        return new Relation(schema, rows);
    }
}
