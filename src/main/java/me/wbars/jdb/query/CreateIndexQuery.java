package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

public class CreateIndexQuery implements Query {
    private final String tableName;
    private final String column;

    public CreateIndexQuery(String tableName, String column) {
        this.tableName = tableName;
        this.column = column;
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.createIndex(tableName, column);
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }
}
