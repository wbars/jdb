package me.wbars.jdb.table;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class TableMeta {
    private String name;
    private List<TableColumn> columns;

    public TableMeta(String name, List<TableColumn> columns) {
        this.name = name;
        this.columns = columns;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public static TableMeta create(String name, List<ColumnData> columns) {
        return new TableMeta(name, new ArrayList<>(columns.stream()
                .map(TableColumn::fromData)
                .collect(toList()))
        );
    }

    public String getName() {
        return name;
    }
}
