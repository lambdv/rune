package com.example.storage;

import org.json.JSONObject;
import org.json.JSONArray;

import com.example.model.Schema;
import com.example.model.Attribute;
import com.example.model.Domain;

import java.util.ArrayList;

public class SchemaJsonSerializer implements SchemaSerializer<JSONObject> {
    public JSONObject serialize(Schema schema) {
        return new JSONObject()
            .put("name", schema.name())
            .put("attributes", new JSONArray(schema.attributes().stream()
                .map(SchemaJsonSerializer::AttributeJson)
                .toList()));
    }
    public Schema deserialize(JSONObject json) {
        var attrs = json.getJSONArray("attributes");
        var list = new ArrayList<Attribute>();
        for (int i = 0; i < attrs.length(); i++) {
            var a = attrs.getJSONObject(i);
            list.add(new Attribute(a.getString("name"), Domain.valueOf(a.getString("domain"))));
        }
        return new Schema(json.getString("name"), list);
    }

    private static JSONObject AttributeJson(Attribute attr){
        var res = new JSONObject();
        res.put("name", attr.name());
        res.put("domain", attr.domain().name());
        return res;
    }
}
