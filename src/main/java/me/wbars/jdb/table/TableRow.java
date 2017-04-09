package me.wbars.jdb.table;

import java.util.List;

public class TableRow {
    private final List<String> values;

    List<String> getValues() {
        return values;
    }

    public TableRow(List<String> values) {
        this.values = values;
    }
}
