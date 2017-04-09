package me.wbars.jdb.scanner;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public class ScannerTest {
    private Scanner scanner;

    @Before
    public void setUp() throws Exception {
        scanner = new Scanner();
    }

    @Test
    public void testCreateTable() throws Exception {
        List<Token> tokens = scanner.scan("create table `test` (" +
                "`id` integer, " +
                "`data` string" +
                ")");
        assertThat(tokens, hasSize(10));
        assertThat(tokens.get(0).type, is(TokenType.CREATE));
        assertThat(tokens.get(1).type, is(TokenType.TABLE));

        assertThat(tokens.get(2).type, is(TokenType.STRING_VAR));
        assertThat(tokens.get(2).value, is("test"));

        assertThat(tokens.get(3).type, is(TokenType.OPEN_PAREN));

        assertThat(tokens.get(4).type, is(TokenType.STRING_VAR));
        assertThat(tokens.get(4).value, is("id"));

        assertThat(tokens.get(5).type, is(TokenType.INTEGER));
        assertThat(tokens.get(6).type, is(TokenType.COMMA));

        assertThat(tokens.get(7).type, is(TokenType.STRING_VAR));
        assertThat(tokens.get(7).value, is("data"));

        assertThat(tokens.get(8).type, is(TokenType.STRING));
        assertThat(tokens.get(9).type, is(TokenType.CLOSE_PAREN));
    }
}