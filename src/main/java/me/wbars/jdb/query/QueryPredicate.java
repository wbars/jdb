package me.wbars.jdb.query;

import me.wbars.jdb.scanner.Token;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;

import java.util.List;
import java.util.function.Function;

import static java.lang.Integer.parseInt;
import static me.wbars.jdb.query.CompareSign.fromAlias;
import static me.wbars.jdb.utils.CollectionsUtils.indexes;

public class QueryPredicate<T extends Comparable<T>> {
    private QueryPredicate and;
    private QueryPredicate or;
    private final String columnName;
    private final CompareSign sign;
    private final Function<String, T> compareMapper;
    private final T valueToCompare;

    public QueryPredicate(String columnName,
                          CompareSign sign,
                          Function<String, T> compareMapper,
                          T valueToCompare) {
        this.columnName = columnName;
        this.sign = sign;
        this.compareMapper = compareMapper;
        this.valueToCompare = valueToCompare;
    }

    public static QueryPredicate<? extends Comparable<?>> create(String columnName, String operator, Token value, Type type) {
        return type == Type.INTEGER ?
                new QueryPredicate<>(columnName, fromAlias(operator), Integer::parseInt, parseInt(value.value))
                : new QueryPredicate<>(columnName, fromAlias(operator), s -> s, value.value);
    }

    public QueryPredicate<T> and(QueryPredicate<? extends Comparable<?>> other) {
        if (and != null) throw new IllegalStateException();
        and = other;
        return this;
    }

    public QueryPredicate<T> or(QueryPredicate<? extends Comparable<?>> other) {
        if (or != null) throw new IllegalStateException();
        or = other;
        return this;
    }

    public final boolean test(List<String> row, List<ColumnData> columns) {
        int compareResult = compareMapper.apply(row.get(getIndex(columnName, columns))).compareTo(valueToCompare);
        if (sign == CompareSign.EQ) return compareResult == 0;
        if (sign == CompareSign.GT) return compareResult > 0;
        if (sign == CompareSign.LT) return compareResult < 0;
        if (sign == CompareSign.LTE) return compareResult <= 0;
        if (sign == CompareSign.GTE) return compareResult >= 0;
        if (sign == CompareSign.NE) return compareResult != 0;
        //todo check if sign was reached
        throw new IllegalArgumentException();
    }

    private static int getIndex(String columnName, List<ColumnData> columns) {
        return indexes(columns)
                .filter(i -> columns.get(i).first.equals(columnName))
                .findAny().orElseThrow(IllegalArgumentException::new);
    }

    public String getColumn() {
        return columnName;
    }

    public CompareSign getSign() {
        return sign;
    }

    public T getValueToCompare() {
        return valueToCompare;
    }

    public boolean isSingle() {
        return and == null && or == null;
    }

    public QueryPredicate and() {
        return and;
    }

    public QueryPredicate or() {
        return or;
    }
}
