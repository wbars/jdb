package me.wbars.jdb.db;

import me.wbars.jdb.table.Table;

public class QueryResult {
    private final boolean ok;
    private final Table table;
    private final String message;

    public static QueryResult ok(Table table) {
        return new QueryResult(true, table, "OK");
    }

    public static QueryResult fail(String message) {
        return new QueryResult(false, null, message);
    }

    private QueryResult(boolean ok, Table table, String message) {
        this.ok = ok;
        this.table = table;
        this.message = message;
    }

    public boolean isOk() {
        return ok;
    }

    public Table getTable() {
        return table;
    }

    public String getMessage() {
        return message;
    }
}
