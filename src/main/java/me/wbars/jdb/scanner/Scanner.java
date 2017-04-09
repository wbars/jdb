package me.wbars.jdb.scanner;


import java.util.ArrayList;
import java.util.List;

public class Scanner {

    private static final int DEFAULT_VALUE = -1;

    public List<Token> scan(String query) throws IllegalArgumentException {
        List<Token> result = new ArrayList<>();
        int start = DEFAULT_VALUE;
        for (int i = 0; i < query.length(); i++) {
            char ch = query.charAt(i);

            if (isWhitespace(ch)) continue;
            if (start == DEFAULT_VALUE) start = i;

            if (i == query.length() - 1 || isWhitespace(query.charAt(i + 1)) || isDelimer(String.valueOf(query.charAt(i + 1)))) {
                result.add(Token.create(query.substring(start, i + 1)));
                start = DEFAULT_VALUE;
                continue;
            }
            if (isDelimer(String.valueOf(ch))) {
                result.add(Token.create(String.valueOf(ch)));
                start = DEFAULT_VALUE;
            }

        }
        return result;
    }

    private boolean isWhitespace(char ch) {
        return ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r';
    }

    private boolean isDelimer(String word) {
        return TokenType.isDelimeter(word);
    }
}
