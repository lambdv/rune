package com.example.core;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;


public record Schema(
    String name,
    List<Attribute> attributes
) {

    public JSONObject toJson(){
        var res = new JSONObject();
        res.put("name", name);
        var attributesJson = new JSONArray();
        for (var a : attributes){
            attributesJson.put(a.toJson());
        }
        res.put("attributes", attributesJson);
        return res;
    }
    public static Schema fromJson(JSONObject json){
        var name = (String) json.get("name");
        var attributes = new ArrayList<Attribute>();
        for (var a : (JSONArray) json.get("attributes")){
            attributes.add(new Attribute((String) ((JSONObject) a).get("name"), Domain.valueOf((String) ((JSONObject) a).get("domain"))));
        }
        var attributeLookup = new HashMap<String, Boolean>(attributes.size() * 2);
        for (var attribute : attributes){
            attributeLookup.put(attribute.name(), Boolean.TRUE);
        }
        return new Schema(name, attributes);
    }
}
