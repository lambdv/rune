package com.example.core;
import org.json.JSONObject;

public record Attribute(
    String name,
    Domain domain
) {

    public JSONObject toJson(){
        var res = new JSONObject();
        res.put("name", name);
        res.put("domain", domain.name());
        return res;
    }
}