package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

public class ShowTablesQuery implements Query {
    public ShowTablesQuery() {
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.executeShowTables();
    }
}
