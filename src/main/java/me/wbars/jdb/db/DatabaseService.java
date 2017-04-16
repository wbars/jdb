package me.wbars.jdb.db;

import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;

import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static me.wbars.jdb.db.QueryResult.fail;
import static me.wbars.jdb.db.QueryResult.ok;
import static me.wbars.jdb.table.Table.create;
import static me.wbars.jdb.utils.CollectionsUtils.toMap;
import static me.wbars.jdb.utils.CollectionsUtils.wrapInLists;

public class DatabaseService {
    private final QueryParser queryParser;
    private final Storage storage;

    public DatabaseService() {
        this(new QueryParser(), new Storage());
    }

    public DatabaseService(QueryParser queryParser, Storage storage) {
        this.queryParser = queryParser;
        this.storage = storage;
    }

    public QueryResult executeQuery(String queryString) {
        return queryParser.parse(queryString).execute(this);
    }

    public QueryResult executeShowTables() {
        return ok(create("tables", singletonList(new ColumnData("table", Type.STRING)), wrapInLists(storage.getTablesNames())));
    }

    public QueryResult executeCreateTable(String tableName, List<ColumnData> columns) {
        if (storage.tableExists(tableName)) return fail(format("Table `%s` already exists", tableName));
        storage.createTable(tableName, columns);
        return ok(null);
    }

    public QueryResult executeDropTable(String tableName) {
        if (!storage.tableExists(tableName)) return fail(String.format("Table `%s` not exists", tableName));
        storage.dropTable(tableName);
        return ok(null);
    }

    public QueryResult executeDescribeTable(String tableName) {
        if (!storage.tableExists(tableName)) return fail(String.format("Table `%s` not exists", tableName));
        return ok(create(tableName, storage.getTableColumns(tableName), emptyList()));
    }

    public QueryResult insert(String tableName, List<String> columns, List<String> rows) {
        if (!storage.tableExists(tableName)) return fail(String.format("Table `%s` not exists", tableName));
        if (columns.size() != rows.size()) throw new IllegalArgumentException();

        Set<String> tableColumns = getColumnsNames(tableName);
        if (!columns.stream().allMatch(tableColumns::contains)) return fail("Some of the columns does not exist");

        storage.insertRow(tableName, toMap(columns, rows));
        return ok(null);
    }

    private Set<String> getColumnsNames(String tableName) {
        return storage.getTableColumns(tableName).stream()
                .map(p -> p.first)
                .collect(toSet());
    }

    public QueryResult select(String tableName, List<String> columns, QueryPredicate predicate) {
        if (!storage.tableExists(tableName)) return fail(String.format("Table `%s` not exists", tableName));
        return ok(create(tableName, getSelectedColumns(tableName, columns), storage.selectRows(tableName, columns, predicate)));
    }

    private List<ColumnData> getSelectedColumns(String tableName, List<String> columns) {
        return storage.getTableColumns(tableName).stream()
                .filter(r -> columns.contains(r.first))
                .collect(toList());
    }

    public QueryResult createIndex(String tableName, String column) {
        if (!storage.tableExists(tableName)) return fail(String.format("Table `%s` not exists", tableName));
        if (storage.indexExists(tableName, column)) return fail("Index exists");
        storage.createIndex(tableName, column);
        return ok(null);
    }
}
