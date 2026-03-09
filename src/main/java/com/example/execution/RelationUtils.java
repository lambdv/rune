package com.example.execution;

import com.example.model.Domain;
import com.example.model.Relation;
import com.example.model.Schema;
import com.example.model.Tuple;

import java.util.ArrayList;
import java.util.List;

public final class RelationUtils {
    private RelationUtils() {}
    
    public static List<Tuple> relationToTuples(Relation relation) {
        var schema = relation.schema();
        return relation.rows().stream()
            .map(row -> {
                var values = new ArrayList<Domain.Value>();
                for (int i = 0; i < row.size(); i++) {
                    var attr = schema.attributes().get(i);
                    var value = parseValue(row.get(i), attr.domain());
                    values.add(value);
                }
                return new Tuple(values);
            })
            .toList();
    }
    
    public static Relation tuplesToRelation(List<Tuple> tuples, Schema schema) {
        var rows = tuples.stream()
            .map(tuple -> tuple.values().stream()
                .map(RelationUtils::valueToString)
                .toList())
            .toList();
        return new Relation(schema, rows);
    }
    
    public static Domain.Value parseValue(String str, Domain domain) {
        if (str == null || str.isEmpty()) {
            return new Domain.NullValue();
        }
        
        return switch (domain) {
            case NUMBER -> new Domain.IntValue(Integer.parseInt(str));
            case SERIAL -> new Domain.IntValue(Integer.parseInt(str));
            case STRING -> new Domain.StringValue(str);
            case BOOLEAN -> new Domain.BoolValue(Boolean.parseBoolean(str));
            case DATE -> new Domain.DateValue(java.time.LocalDate.parse(str));
        };
    }
    
    public static String valueToString(Domain.Value value) {
        if (value instanceof Domain.IntValue iv) {
            return String.valueOf(iv.v());
        } else if (value instanceof Domain.StringValue sv) {
            return sv.v();
        } else if (value instanceof Domain.BoolValue bv) {
            return String.valueOf(bv.v());
        } else if (value instanceof Domain.NullValue) {
            return "";
        } else if (value instanceof Domain.DateValue dv) {
            return dv.v().toString();
        } else {
            throw new IllegalArgumentException("Unknown value type: " + value.getClass().getSimpleName());
        }
    }
}
