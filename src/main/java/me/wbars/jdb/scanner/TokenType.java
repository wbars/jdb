package me.wbars.jdb.scanner;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public enum TokenType {
    SHOW(Pattern.compile("^show$")),
    TABLE(Pattern.compile("^table$")),
    TABLES(Pattern.compile("^tables$")),
    INSERT(Pattern.compile("^insert$")),
    INTO(Pattern.compile("^into$")),
    INTEGER(Pattern.compile("^integer$")),
    STRING(Pattern.compile("^string$")),
    CREATE(Pattern.compile("^create$")),
    DROP(Pattern.compile("^drop$")),
    DESCRIBE(Pattern.compile("^describe$")),
    SEMICOLON(Pattern.compile("^;$")) {
        @Override
        public boolean isDelimeter() {
            return true;
        }
    },
    COMMA(Pattern.compile("^,$")) {
        @Override
        public boolean isDelimeter() {
            return true;
        }
    },
    OPEN_PAREN(Pattern.compile("^\\($")) {
        @Override
        public boolean isDelimeter() {
            return true;
        }
    },
    CLOSE_PAREN(Pattern.compile("^\\)$")) {
        @Override
        public boolean isDelimeter() {
            return true;
        }
    },
    UNSIGNED_INTEGER(Pattern.compile("^\\d+$")),
    STRING_VAR(Pattern.compile("^`.+`$")) {
        @Override
        public String extractValue(String word) {
            return word.substring(1, word.length() - 1);
        }
    },
    VALUES(Pattern.compile("^values$")),
    SELECT(Pattern.compile("^select$")),
    FROM(Pattern.compile("^from$")),
    WHERE(Pattern.compile("^where$")),
    RELOP(Pattern.compile("^>|<|<=|>=|=|!=$")),
    BOOLEAN_RELOP(Pattern.compile("^and|or$")),
    INDEX(Pattern.compile("^index$")), ON(Pattern.compile("^on$"));

    private final Pattern pattern;

    TokenType(Pattern pattern) {
        this.pattern = pattern;
    }

    public static List<TokenType> types() {
        return Arrays.asList(values());
    }

    public String extractValue(String word) {
        return word;
    }

    public boolean isDelimeter() {
        return false;
    }

    public boolean test(String word) {
        return pattern.asPredicate().test(word);
    }

    public static boolean isDelimeter(String word) {
        return types().stream()
                .filter(TokenType::isDelimeter)
                .anyMatch(r -> r.test(word));
    }

}
