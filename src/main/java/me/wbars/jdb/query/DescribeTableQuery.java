package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

public class DescribeTableQuery implements Query {
    private final String tableName;

    public DescribeTableQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.executeDescribeTable(tableName);
    }

    public String getTableName() {
        return tableName;
    }
}
