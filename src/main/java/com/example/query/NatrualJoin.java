package com.example.query;

import java.util.function.BiFunction;

import com.example.model.Schema;
import com.example.model.Attribute;
import java.util.ArrayList;

public record NatrualJoin(RANode left, RANode right) implements JoinNode {}