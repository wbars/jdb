package me.wbars.jdb.db;


import me.wbars.jdb.query.*;
import me.wbars.jdb.scanner.Scanner;
import me.wbars.jdb.scanner.Token;
import me.wbars.jdb.scanner.TokenType;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.Math.min;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static me.wbars.jdb.db.QueryPredicateFactory.create;
import static me.wbars.jdb.scanner.TokenType.*;
import static me.wbars.jdb.utils.CollectionsUtils.concat;
import static me.wbars.jdb.utils.CollectionsUtils.indexes;

public class QueryParser {
    private final List<Pair<Predicate<List<Token>>, Function<List<Token>, Query>>> prefixQueryProcessors = new ArrayList<>();
    private final Scanner scanner = new Scanner();

    public QueryParser() {
        prefixQueryProcessors.add(new Pair<>(this::isValidShowTables, tokens -> new ShowTablesQuery()));
        prefixQueryProcessors.add(new Pair<>(this::isValidCreateTable, this::createCreateTables));
        prefixQueryProcessors.add(new Pair<>(this::isValidDropTable, this::createDropTable));
        prefixQueryProcessors.add(new Pair<>(this::isValidDescribeTable, this::createDescribeTable));
        prefixQueryProcessors.add(new Pair<>(this::isValidInsert, this::createInsert));
        prefixQueryProcessors.add(new Pair<>(this::isValidSelect, this::createSelect));
    }

    private boolean hasPrefix(List<Token> base, TokenType... prefixTypes) {
        return base.size() >= prefixTypes.length && areTypesMatches(base, prefixTypes);
    }

    private boolean areTypesMatches(List<Token> tokens, TokenType[] types) {
        return indexes(types).allMatch(i -> tokens.get(i).type == types[i]);
    }

    private boolean isValidShowTables(List<Token> tokens) {
        return tokens.size() == 2 && hasPrefix(tokens, SHOW, TABLES);
    }

    private boolean isValidInsert(List<Token> tokens) {
        return hasPrefix(tokens, INSERT, INTO);
    }

    private boolean isValidSelect(List<Token> tokens) {
        return hasPrefix(tokens, SELECT);
    }

    private boolean isValidDescribeTable(List<Token> tokens) {
        return tokens.size() == 3 && hasPrefix(tokens, DESCRIBE, TABLE);
    }

    private DescribeTableQuery createDescribeTable(List<Token> tokens) {
        return new DescribeTableQuery(getTokenAsType(tokens, 2, TokenType.STRING_VAR).value);
    }

    private InsertQuery createInsert(List<Token> tokens) {
        String tableName = getTokenAsType(tokens, 2, TokenType.STRING_VAR).value;
        int valuesKeywordIndex = getIndexOfToken(tokens, TokenType.VALUES);
        if (valuesKeywordIndex < 0) throw new IllegalArgumentException();

        return new InsertQuery(
                parseValues(unwrap(tokens.subList(3, valuesKeywordIndex))),
                parseValues(unwrap(tokens.subList(valuesKeywordIndex + 1, tokens.size()))),
                tableName
        );
    }

    private List<String> parseValues(List<Token> tokens) {
        if (tokens.isEmpty()) return emptyList();
        String head = tokens.get(0).value;
        if (tokens.size() > 1 && tokens.get(1).type != TokenType.COMMA)
            throw new IllegalArgumentException(tokens.get(1).value);
        return concat(head, parseValues(tokens.subList(min(2, tokens.size()), tokens.size())));
    }

    private static int getIndexOfToken(List<Token> tokens, TokenType type) {
        return indexes(tokens)
                .filter(i -> tokens.get(i).type == type)
                .findFirst().orElse(-1);
    }

    private boolean isValidCreateTable(List<Token> tokens) {
        return tokens.size() >= 3 && hasPrefix(tokens, CREATE, TABLE);
    }

    private CreateTableQuery createCreateTables(List<Token> tokens) {
        String tableName = getTokenAsType(tokens, 2, TokenType.STRING_VAR).value;
        if (tokens.size() == 3) return new CreateTableQuery(tableName);

        return new CreateTableQuery(tableName, parseColumns(unwrap(tokens.subList(3, tokens.size()))));
    }

    private SelectQuery createSelect(List<Token> tokens) {
        int fromKeywordIndex = getIndexOfToken(tokens, TokenType.FROM);
        if (fromKeywordIndex < 0) throw new IllegalArgumentException();

        String tableName = getTokenAsType(tokens, fromKeywordIndex + 1, TokenType.STRING_VAR).value;
        if (tokens.size() == fromKeywordIndex + 2)
            return new SelectQuery(tableName, parseValues(unwrap(tokens.subList(1, fromKeywordIndex))));

        int whereIndex = getIndexOfToken(tokens, TokenType.WHERE);
        if (whereIndex < 0) throw new IllegalArgumentException();
        return new SelectQuery(
                tableName,
                parseValues(unwrap(tokens.subList(1, fromKeywordIndex))),
                parseWherePredicate(tokens.subList(whereIndex + 1, tokens.size()))
        );
    }

    private QueryPredicate parseWherePredicate(List<Token> tokens) {
        Token columnName = getTokenAsType(tokens, 0, TokenType.STRING_VAR);
        Token operator = getTokenAsType(tokens, 1, TokenType.RELOP);
        Token value = getTokenAnyOfTypes(tokens, 2, TokenType.STRING_VAR, TokenType.UNSIGNED_INTEGER);
        return accumulatePredicate(tokens.subList(3, tokens.size()), create(columnName.value, operator.value, value));
    }

    private QueryPredicate accumulatePredicate(List<Token> tokens, QueryPredicate predicate) {
        while (!tokens.isEmpty()) {
            if (getTokenAsType(tokens, 0, TokenType.BOOLEAN_RELOP).value.equals("or"))
                return predicate.or(parseWherePredicate(tokens.subList(1, tokens.size())));

            predicate = predicate.and(parseWherePredicate(tokens.subList(1, 4)));
            tokens = tokens.subList(4, tokens.size());
        }
        return predicate;
    }

    private List<ColumnData> parseColumns(List<Token> tokens) {
        if (tokens.isEmpty()) return emptyList();
        if (tokens.size() < 2) throw new IllegalArgumentException("Invalid number of arguments");

        ColumnData head = new ColumnData(getTokenAsType(tokens, 0, STRING_VAR).value, Type.fromString(tokens.get(1).value));
        return concat(head, parseColumns(tokens.subList(min(3, tokens.size()), tokens.size())));
    }

    private List<Token> unwrap(List<Token> tokens) {
        return tokens.subList(1, tokens.size() - 1);
    }

    private DropTableQuery createDropTable(List<Token> tokens) {
        return new DropTableQuery(getTokenAsType(tokens, 2, TokenType.STRING_VAR).value);
    }

    private boolean isValidDropTable(List<Token> tokens) {
        return tokens.size() == 3 && hasPrefix(tokens, DROP, TABLE);
    }

    private Token getTokenAsType(List<Token> tokens, int index, TokenType type) {
        Token token = tokens.get(index);
        if (token.type != type) throw new IllegalArgumentException(token.value);
        return token;
    }

    private Token getTokenAnyOfTypes(List<Token> tokens, int index, TokenType... types) {
        Token token = tokens.get(index);
        Set<TokenType> typesSet = stream(types).collect(toSet());
        if (!typesSet.contains(token.type)) throw new IllegalArgumentException(token.value);
        return token;
    }

    public Query parse(String queryString) {
        List<Token> tokens = scanner.scan(queryString);
        return prefixQueryProcessors.stream()
                .filter(p -> p.first.test(tokens))
                .map(p -> p.second)
                .findFirst().orElseThrow(() -> new IllegalArgumentException(queryString))
                .apply(tokens);
    }
}
