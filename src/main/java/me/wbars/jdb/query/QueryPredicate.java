package me.wbars.jdb.query;

import me.wbars.jdb.scanner.Token;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;

import java.util.List;
import java.util.function.Function;

import static java.lang.Integer.parseInt;
import static me.wbars.jdb.query.CompareSign.fromAlias;
import static me.wbars.jdb.utils.CollectionsUtils.indexes;

public class QueryPredicate<T extends Comparable> {
    private QueryPredicate and;
    private QueryPredicate or;
    private final String columnName;
    private final CompareSign sign;
    private final Function<String, Comparable<T>> compareMapper;
    private final T valueToCompare;

    public QueryPredicate(String columnName,
                          CompareSign sign,
                          Function<String, Comparable<T>> compareMapper,
                          T valueToCompare) {
        this.columnName = columnName;
        this.sign = sign;
        this.compareMapper = compareMapper;
        this.valueToCompare = valueToCompare;
    }

    public static QueryPredicate create(String columnName, String operator, Token value, Type type) {
        return type == Type.INTEGER ?
                new QueryPredicate<>(columnName, fromAlias(operator), Integer::parseInt, parseInt(value.value))
                : new QueryPredicate<>(columnName, fromAlias(operator), s -> s, value.value);
    }

    public QueryPredicate and(QueryPredicate other) {
        if (and != null) throw new IllegalStateException();
        and = other;
        return this;
    }

    public QueryPredicate or(QueryPredicate other) {
        if (or != null) throw new IllegalStateException();
        or = other;
        return this;
    }

    public final boolean test(List<String> row, List<ColumnData> columns) {
        int compareResult = compareMapper.apply(row.get(getIndex(columnName, columns))).compareTo(valueToCompare);
        boolean result = false;
        if (sign == CompareSign.EQ) result = compareResult == 0;
        if (sign == CompareSign.GT) result = compareResult > 0;
        if (sign == CompareSign.LT) result = compareResult < 0;
        if (sign == CompareSign.LTE) result = compareResult <= 0;
        if (sign == CompareSign.GTE) result = compareResult >= 0;
        if (sign == CompareSign.NE) result = compareResult != 0;
        //todo check if sign was reached

        return result
                && (and == null || and.test(row, columns))
                || (or != null && or.test(row, columns));
    }

    private static int getIndex(String columnName, List<ColumnData> columns) {
        return indexes(columns)
                .filter(i -> columns.get(i).first.equals(columnName))
                .findAny().orElseThrow(IllegalArgumentException::new);
    }
}
