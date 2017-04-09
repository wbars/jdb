package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

import java.util.List;

public class InsertQuery implements Query {
    private final List<String> columns;
    private final List<String> values;
    private final String tableName;

    public InsertQuery(List<String> columns, List<String> values, String tableName) {
        this.columns = columns;
        this.values = values;
        this.tableName = tableName;
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.insert(tableName, columns, values);
    }
}
