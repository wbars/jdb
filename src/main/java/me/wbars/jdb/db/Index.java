package me.wbars.jdb.db;

import me.wbars.jdb.query.CompareSign;
import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import me.wbars.jdb.utils.CollectionsUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Stream.concat;
import static me.wbars.jdb.query.CompareSign.*;

public class Index<T extends Comparable<T>> {
    private final BTree<T> bTree;

    public Index(BTree<T> bTree) {
        this.bTree = bTree;
    }

    public Stream<List<String>> scan(QueryPredicate<T> predicate, List<List<String>> rows) {
        return getIndexes(bTree, predicate).map(rows::get);
    }

    private Stream<Integer> getIndexes(BTree<T> bTree, final QueryPredicate<T> predicate) {
        if (bTree == null) return indexes(null);

        int compare = bTree.getValue().compareTo(predicate.getValueToCompare());
        CompareSign sign = predicate.getSign();
        BTree<T> right = bTree.getRight();
        BTree<T> left = bTree.getLeft();
        if (compare == 0) {
            if (sign == EQ) return indexes(bTree);
            if (sign == GTE) return concat(indexes(bTree), allIndexes(right));
            if (sign == LTE) return concat(indexes(bTree), allIndexes(left));
            if (sign == GT) return allIndexes(right);
            if (sign == LT) return allIndexes(left);
        }

        if (compare > 0) {
            if (sign == EQ || sign == LTE || sign == LT) return getIndexes(left, predicate);
            if (sign == GTE || sign == GT)
                return concat(allIndexes(bTree.getRight()), concat(indexes(bTree), getIndexes(left, predicate)));
        }

        // < 0
        if (sign == EQ || sign == GTE || sign == GT) return getIndexes(right, predicate);
        if (sign == LTE || sign == LT)
            return concat(allIndexes(bTree.getLeft()), concat(indexes(bTree), getIndexes(right, predicate)));

        throw new IllegalArgumentException();
    }

    private Stream<Integer> allIndexes(BTree<T> btree) {
        return btree != null ? concat(indexes(btree), concat(allIndexes(btree.getRight()), allIndexes(btree.getLeft()))) : Stream.empty();
    }

    private Stream<Integer> indexes(BTree<T> btree) {
        return btree != null ? btree.getRowsIndexes().stream() : Stream.empty();
    }

    public static <T extends Comparable<T>> Index<T> create(Table table, String column, Function<String, T> mapper) {
        return new Index<>(createBTree(mapper, getIndex(column, table.getColumns()), table.getRows()));
    }

    private static <T extends Comparable<T>> BTree<T> createBTree(Function<String, T> mapper, int index, List<List<String>> rows) {
        int mid = rows.size() / 2; //todo balance
        BTree<T> bTree = new BTree<>(mapper.apply(rows.get(mid).get(index))); //handle null
        bTree.getRowsIndexes().add(mid);
        for (int i = 0; i < rows.size(); i++) {
            if (i != mid) bTree.insert(mapper.apply(rows.get(i).get(index)), i);
        }
        return bTree;
    }

    private static int getIndex(String columnName, List<ColumnData> columns) {
        return CollectionsUtils.indexes(columns)
                .filter(i -> columns.get(i).first.equals(columnName))
                .findAny().orElseThrow(IllegalArgumentException::new);
    }
}
