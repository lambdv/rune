package com.example.execution;

import com.example.model.Relation;
import com.example.model.Schema;
import com.example.query.*;
import com.example.storage.PersistancyManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public record RAExecutor(String basePath) implements Executor {
    
    @Override
    public Relation execute(RANode node) {
        return switch (node) {
            case SelectNode selectNode -> executeSelect(selectNode);
            case ProjectionNode projectionNode -> executeProjection(projectionNode);
            case RenameNode renameNode -> executeRename(renameNode);
            case UnionNode unionNode -> executeUnion(unionNode);
            case DifferenceNode differenceNode -> executeDifference(differenceNode);
            case NatrualJoin naturalJoin -> executeNaturalJoin(naturalJoin);
            default -> throw new IllegalArgumentException("Unsupported node type: " + node.getClass().getSimpleName());
        };
    }
    
    private Relation executeSelect(SelectNode node) {
        var schema = node.schema();
        var filename = basePath + "/" + schema.name() + ".csv";
        var relation = PersistancyManager.loadRelation(schema, filename);
        if (relation == null) {
            throw new IllegalStateException("Failed to load relation from: " + filename);
        }
        return relation;
    }
    
    private Relation executeProjection(ProjectionNode node) {
        var childRelation = execute(node.child());
        var childSchema = childRelation.schema();
        var projectionSchema = node.schema();
        
        var attributeIndices = projectionSchema.attributes().stream()
            .map(attr -> childSchema.attributes().indexOf(attr))
            .toList();
        
        var projectedRows = childRelation.rows().stream()
            .map(row -> attributeIndices.stream()
                .map(row::get)
                .toList())
            .toList();
        
        return new Relation(projectionSchema, projectedRows);
    }
    
    private Relation executeRename(RenameNode node) {
        var childRelation = execute(node.child());
        var renamedSchema = node.schema();
        
        return new Relation(renamedSchema, childRelation.rows());
    }
    
    private Relation executeUnion(UnionNode node) {
        var leftRelation = execute(node.left());
        var rightRelation = execute(node.right());
        var resultSchema = node.schema();
        
        var leftTuples = RelationUtils.relationToTuples(leftRelation);
        var rightTuples = RelationUtils.relationToTuples(rightRelation);
        
        var unionTuples = new ArrayList<com.example.model.Tuple>();
        var seenTuples = new java.util.HashSet<com.example.model.Tuple>();
        
        for (var tuple : leftTuples) {
            if (!seenTuples.contains(tuple)) {
                unionTuples.add(tuple);
                seenTuples.add(tuple);
            }
        }
        
        for (var tuple : rightTuples) {
            if (!seenTuples.contains(tuple)) {
                unionTuples.add(tuple);
                seenTuples.add(tuple);
            }
        }
        
        return RelationUtils.tuplesToRelation(unionTuples, resultSchema);
    }
    
    private Relation executeDifference(DifferenceNode node) {
        var leftRelation = execute(node.left());
        var rightRelation = execute(node.right());
        var resultSchema = node.schema();
        
        var leftTuples = RelationUtils.relationToTuples(leftRelation);
        var rightTuples = RelationUtils.relationToTuples(rightRelation);
        var rightSet = Set.copyOf(rightTuples);
        
        var differenceTuples = leftTuples.stream()
            .filter(tuple -> !rightSet.contains(tuple))
            .toList();
        
        return RelationUtils.tuplesToRelation(differenceTuples, resultSchema);
    }
    
    private Relation executeNaturalJoin(NatrualJoin node) {
        var leftRelation = execute(node.left());
        var rightRelation = execute(node.right());
        var resultSchema = node.schema();
        
        var leftSchema = leftRelation.schema();
        var rightSchema = rightRelation.schema();
        
        var commonAttributes = leftSchema.attributes().stream()
            .filter(rightSchema.attributes()::contains)
            .toList();
        
        var leftCommonIndices = commonAttributes.stream()
            .map(leftSchema.attributes()::indexOf)
            .toList();
        
        var rightCommonIndices = commonAttributes.stream()
            .map(rightSchema.attributes()::indexOf)
            .toList();
        
        var joinedRows = new ArrayList<List<String>>();
        
        for (var leftRow : leftRelation.rows()) {
            var leftCommonValues = leftCommonIndices.stream()
                .map(leftRow::get)
                .toList();
            
            for (var rightRow : rightRelation.rows()) {
                var rightCommonValues = rightCommonIndices.stream()
                    .map(rightRow::get)
                    .toList();
                
                if (leftCommonValues.equals(rightCommonValues)) {
                    var joinedRow = new ArrayList<String>();
                    
                    for (var attr : resultSchema.attributes()) {
                        if (leftSchema.attributes().contains(attr)) {
                            var leftIdx = leftSchema.attributes().indexOf(attr);
                            joinedRow.add(leftRow.get(leftIdx));
                        } else {
                            var rightIdx = rightSchema.attributes().indexOf(attr);
                            joinedRow.add(rightRow.get(rightIdx));
                        }
                    }
                    
                    joinedRows.add(joinedRow);
                }
            }
        }
        
        return new Relation(resultSchema, joinedRows);
    }
}
