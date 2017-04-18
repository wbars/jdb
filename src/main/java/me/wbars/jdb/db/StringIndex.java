package me.wbars.jdb.db;

public class StringIndex extends Index<String> {
    public StringIndex(BTree<String> bTree) {
        super(bTree);
    }

    @Override
    String mapValue(String s) {
        return s;
    }
}
