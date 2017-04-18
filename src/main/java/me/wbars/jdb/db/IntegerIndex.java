package me.wbars.jdb.db;

public class IntegerIndex extends Index<Integer> {

    public IntegerIndex(BTree<Integer> bTree) {
        super(bTree);
    }

    @Override
    Integer mapValue(String s) {
        return Integer.parseInt(s);
    }
}
