package me.wbars.jdb.table;

import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.utils.Pair;

public class ColumnData extends Pair<String, Type> {
    public ColumnData(String first, Type second) {
        super(first, second);
    }
}
