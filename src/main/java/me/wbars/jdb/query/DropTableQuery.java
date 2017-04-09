package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

public class DropTableQuery implements Query {
    private final String tableName;

    public DropTableQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.executeDropTable(tableName);
    }

    public String getTableName() {
        return tableName;
    }
}
