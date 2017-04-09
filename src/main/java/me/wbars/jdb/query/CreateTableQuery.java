package me.wbars.jdb.query;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;
import me.wbars.jdb.table.ColumnData;

import java.util.ArrayList;
import java.util.List;

public class CreateTableQuery implements Query {
    private final String tableName;
    private List<ColumnData> columns;

    public CreateTableQuery(String tableName) {
        this(tableName, new ArrayList<>());
    }

    public CreateTableQuery(String tableName, List<ColumnData> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public QueryResult execute(DatabaseService service) {
        return service.executeCreateTable(tableName, columns);
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnData> getColumns() {
        return columns;
    }
}
