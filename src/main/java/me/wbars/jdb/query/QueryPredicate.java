package me.wbars.jdb.query;

import me.wbars.jdb.table.ColumnData;

import java.util.List;

public interface QueryPredicate {
    default QueryPredicate and(QueryPredicate other) {
        return (row, columns) -> test(row, columns) && other.test(row, columns);
    }

    default QueryPredicate or(QueryPredicate other) {
        return (row, columns) -> test(row, columns) || other.test(row, columns);
    }

    boolean test(List<String> row, List<ColumnData> columns);
}
