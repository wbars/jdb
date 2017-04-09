package me.wbars.jdb.table;

import me.wbars.jdb.scanner.Type;

public class TableColumn {
    private final String name;
    private final Type type;

    public TableColumn(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public static TableColumn string(String name) {
        return new TableColumn(name, Type.STRING);
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public ColumnData toData() {
        return new ColumnData(name, type);
    }

    public static TableColumn fromData(ColumnData data) {
        return new TableColumn(data.first, data.second);
    }
}
