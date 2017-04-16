package me.wbars.jdb.db;

import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import me.wbars.jdb.table.TableRow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static me.wbars.jdb.utils.CollectionsUtils.indexes;
import static me.wbars.jdb.utils.CollectionsUtils.withIndexes;

public class Storage {
    private final Map<String, Table> tables = new HashMap<>();

    public void createTable(String tableName, List<ColumnData> columns) {
        tables.put(tableName, Table.create(tableName, columns, emptyList()));
    }

    public List<String> getTablesNames() {
        return tables.values().stream()
                .map(Table::getName)
                .collect(toList());
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    public void dropTable(String tableName) {
        tables.remove(tableName);
    }

    public List<ColumnData> getTableColumns(String tableName) {
        return tables.get(tableName).getColumns();
    }

    public List<String> getTableColumnsNames(String tableName) {
        return tables.get(tableName).getColumns().stream().map(p -> p.first).collect(toList());
    }

    public void insertRow(String tableName, Map<String, String> columnsAndRows) {
        Table table = tables.get(tableName);
        table.addRow(
                new TableRow(
                        table.getColumns().stream()
                                .map(c -> columnsAndRows.get(c.first))
                                .collect(toList())
                )
        );
    }

    public List<List<String>> selectAllRows(String tableName) {
        return selectRows(tableName, getTableColumnsNames(tableName), null);
    }

    public List<List<String>> selectRows(String tableName, List<String> columns, QueryPredicate predicate) {
        Table table = tables.get(tableName);
        List<String> tableColumnsNames = getTableColumnsNames(tableName);

        Set<Integer> columnsIndexesToInclude = indexes(tableColumnsNames)
                .filter(i -> columns.contains(tableColumnsNames.get(i)))
                .collect(toSet());

        List<ColumnData> tableColumns = getTableColumns(tableName);

        return table.getRows().stream()
                .filter(values -> predicate == null || predicate.test(values, tableColumns))
                .map(values -> withIndexes(values, columnsIndexesToInclude))
                .collect(toList());
    }
}
