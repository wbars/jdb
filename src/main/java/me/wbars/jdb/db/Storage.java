package me.wbars.jdb.db;

import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import me.wbars.jdb.table.TableRow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static me.wbars.jdb.query.CompareSign.NE;
import static me.wbars.jdb.utils.CollectionsUtils.indexes;
import static me.wbars.jdb.utils.CollectionsUtils.withIndexes;

public class Storage {
    private final Map<String, Table> tables = new HashMap<>();
    private final HashMap<String, Map<String, Index<? extends Comparable<?>>>> indexes = new HashMap<>();

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

    public List<List<String>> selectRows(String tableName, List<String> columns, QueryPredicate<? extends Comparable<?>> predicate) {
        List<String> tableColumnsNames = getTableColumnsNames(tableName);

        Set<Integer> columnsIndexesToInclude = indexes(tableColumnsNames)
                .filter(i -> columns.contains(tableColumnsNames.get(i)))
                .collect(toSet());
        return tryIndexScan(tableName, predicate, getTableColumns(tableName))
                .map(values -> withIndexes(values, columnsIndexesToInclude))
                .collect(toList());
    }

    public void createIndex(String tableName, String column) {
        if (!tables.containsKey(tableName)) throw new IllegalArgumentException("Table does not exists");
        if (indexes.getOrDefault(tableName, emptyMap()).containsKey(column))
            throw new IllegalArgumentException("Index already exists");

        if (getType(column, getTableColumns(tableName)) == Type.STRING) {
            indexes.compute(tableName, (s, b) -> new HashMap<>()).put(column, Index.create(tables.get(tableName), column, s -> s));
        } else {
            indexes.compute(tableName, (s, b) -> new HashMap<>()).put(column, Index.create(tables.get(tableName), column, Integer::parseInt));
        }
    }

    private static Type getType(String columnName, List<ColumnData> columns) {
        return columns.stream()
                .filter(r -> r.first.equals(columnName))
                .map(r -> r.second).findAny()
                .orElseThrow(IllegalArgumentException::new);
    }

    private Stream<List<String>> tryIndexScan(String tableName, QueryPredicate predicate, List<ColumnData> tableColumns) {
        Index<? extends Comparable<?>> index = predicate != null ? indexes.getOrDefault(tableName, emptyMap()).get(predicate.getColumn()) : null;
        return index != null && predicate.getSign() != NE ? index.scan(predicate, tables.get(tableName).getRows()) : seqScan(predicate, tables.get(tableName), tableColumns);
    }

    private Stream<List<String>> seqScan(QueryPredicate predicate, Table table, List<ColumnData> tableColumns) {
        return table.getRows().stream()
                .filter(values -> predicate == null || predicate.test(values, tableColumns));
    }

    public boolean indexExists(String tableName, String column) {
        return indexes.getOrDefault(tableName, emptyMap()).containsKey(column);
    }
}
