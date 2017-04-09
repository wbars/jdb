package me.wbars.jdb.table;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class Table {
    private final TableMeta meta;
    private final List<TableRow> rows;

    private Table(TableMeta meta, List<TableRow> rows) {
        this.meta = meta;
        this.rows = rows;
    }

    public List<List<String>> getRows() {
        return rows.stream()
                .map(TableRow::getValues)
                .collect(toList());
    }

    public static Table create(String name, List<ColumnData> columns, List<List<String>> rows) {
        return new Table(TableMeta.create(name, columns), new ArrayList<>(rows.stream().map(TableRow::new).collect(toList())));
    }

    public String getName() {
        return meta.getName();
    }

    public List<ColumnData> getColumns() {
        return meta.getColumns().stream()
                .map(TableColumn::toData)
                .collect(toList());
    }

    public void addRow(TableRow row) {
        rows.add(row);
    }
}
