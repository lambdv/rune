package com.example.model;

import java.time.LocalDate;

public enum Domain {
    NUMBER, STRING, BOOLEAN, SERIAL, DATE;

    public sealed interface Value permits IntValue, StringValue, BoolValue, NullValue, DateValue {}
    public record IntValue(int v) implements Value {}
    public record StringValue(String v) implements Value {}
    public record BoolValue(boolean v) implements Value {}
    public record NullValue() implements Value {}   
    public record DateValue(LocalDate v) implements Value {}
}
