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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static me.wbars.jdb.utils.CollectionsUtils.*;

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
        List<String> row = table.getColumns().stream()
                .map(c -> columnsAndRows.get(c.first))
                .collect(toList());
        table.addRow(new TableRow(row));
        Map<String, Index<? extends Comparable<?>>> indexes = this.indexes.getOrDefault(tableName, emptyMap());
        columnsAndRows.entrySet().stream()
                .filter(r -> r.getValue() != null && indexes.containsKey(r.getKey()))
                .forEach(r -> indexes.get(r.getKey()).insert(r.getValue(), table.getRows().size() - 1));
    }

    public List<List<String>> selectAllRows(String tableName) {
        return selectRows(tableName, getTableColumnsNames(tableName), null);
    }

    public List<List<String>> selectRows(String tableName, List<String> columns, QueryPredicate<? extends Comparable<?>> predicate) {
        List<String> tableColumnsNames = getTableColumnsNames(tableName);

        Set<Integer> columnsIndexesToInclude = indexes(tableColumnsNames)
                .filter(i -> columns.contains(tableColumnsNames.get(i)))
                .collect(toSet());
        List<List<String>> rows = tables.get(tableName).getRows();
        return tryIndexScan(tableName, predicate, getTableColumns(tableName)).stream()
                .sorted()
                .map(i -> withIndexes(rows.get(i), columnsIndexesToInclude))
                .collect(toList());
    }

    public void createIndex(String tableName, String column) {
        if (!tables.containsKey(tableName)) throw new IllegalArgumentException("Table does not exists");
        if (indexes.getOrDefault(tableName, emptyMap()).containsKey(column))
            throw new IllegalArgumentException("Index already exists");

        if (getType(column, getTableColumns(tableName)) == Type.STRING) {
            indexes.compute(tableName, (s, b) -> new HashMap<>()).put(column, Index.createStringIndex(tables.get(tableName), column));
        } else {
            indexes.compute(tableName, (s, b) -> new HashMap<>()).put(column, Index.createIntIndex(tables.get(tableName), column));
        }
    }

    private static Type getType(String columnName, List<ColumnData> columns) {
        return columns.stream()
                .filter(r -> r.first.equals(columnName))
                .map(r -> r.second).findAny()
                .orElseThrow(IllegalArgumentException::new);
    }

    private List<Integer> tryIndexScan(String tableName, QueryPredicate predicate, List<ColumnData> tableColumns) {
        Index<? extends Comparable<?>> index = predicate != null ? indexes.getOrDefault(tableName, emptyMap()).get(predicate.getColumn()) : null;
        List<Integer> indexes = index != null ? index.scan(predicate) : seqScan(predicate, tables.get(tableName), tableColumns);
        if (predicate == null) return indexes;

        List<Integer> and = predicate.and() != null ? tryIndexScan(tableName, predicate.and(), tableColumns) : indexes;
        List<Integer> or = predicate.or() != null ? tryIndexScan(tableName, predicate.or(), tableColumns) : emptyList();
        return union(intersection(indexes, and), or);
    }

    private List<Integer> seqScan(QueryPredicate predicate, Table table, List<ColumnData> tableColumns) {
        List<List<String>> rows = table.getRows();
        return predicate != null ? indexes(rows).filter(i -> predicate.test(rows.get(i), tableColumns)).collect(toList()) : indexes(rows).collect(toList());
    }

    public boolean indexExists(String tableName, String column) {
        return indexes.getOrDefault(tableName, emptyMap()).containsKey(column);
    }
}
