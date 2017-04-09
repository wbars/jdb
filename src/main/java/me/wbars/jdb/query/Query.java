package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;

public interface Query {
    QueryResult execute(DatabaseService service);
}
