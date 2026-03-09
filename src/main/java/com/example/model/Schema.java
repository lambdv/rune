package com.example.model;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;


public record Schema(
    String name,
    List<Attribute> attributes
) {



}
