package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

import java.util.List;

public class SelectQuery implements Query {
    public static final QueryPredicate ACCEPT_ALL = (row, columns) -> true;

    private final String tableName;
    private final List<String> columns;
    private final QueryPredicate predicate;

    public SelectQuery(String tableName, List<String> columns, QueryPredicate predicate) {
        this.tableName = tableName;
        this.columns = columns;
        this.predicate = predicate;
    }

    public SelectQuery(String tableName, List<String> columns) {
        this(tableName, columns, ACCEPT_ALL);
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.select(tableName, columns, predicate);
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }
}
