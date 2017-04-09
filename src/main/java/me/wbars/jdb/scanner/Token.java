package me.wbars.jdb.scanner;

public class Token {
    public final TokenType type;
    public final String value;

    private Token(TokenType type, String value) {
        this.type = type;
        this.value = value;
    }

    public static Token create(String word) {
        TokenType type = TokenType.types().stream()
                .filter(t -> t.test(word))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(word));
        return new Token(type, type.extractValue(word));
    }
}
