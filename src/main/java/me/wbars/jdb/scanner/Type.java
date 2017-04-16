package me.wbars.jdb.scanner;

import java.util.Set;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;

public enum Type {
    STRING, INTEGER;


    public static Type fromString(String value) {
        return valueOf(value.toUpperCase());
    }

    public static Set<Type> all() {
        return stream(values()).collect(toSet());
    }

    public static Type fromToken(Token value) {
        if (value.type == TokenType.UNSIGNED_INTEGER) return INTEGER;
        if (value.type == TokenType.STRING_VAR) return STRING;
        throw new IllegalArgumentException();
    }
}
