package me.wbars.jdb.db;

import me.wbars.jdb.query.CompareSign;
import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import me.wbars.jdb.utils.CollectionsUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static me.wbars.jdb.query.CompareSign.*;
import static me.wbars.jdb.utils.CollectionsUtils.distinctBy;

public class Index<T extends Comparable<T>> {
    private final BTree<T> bTree;

    public Index(BTree<T> bTree) {
        this.bTree = bTree;
    }

    public Stream<List<String>> scan(QueryPredicate<T> predicate, List<List<String>> rows) {
        return getIndexes(bTree, predicate).sorted().map(rows::get);
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

    //todo test
    public static <T extends Comparable<T>> List<List<String>> sortByColumn(List<List<String>> rows, int index, Function<String, T> mapper) {
        return rows.stream()
                .sorted(Comparator.comparing(s -> mapper.apply(s.get(index))))
                .collect(Collectors.toList());
    }

    private static <T extends Comparable<T>> BTree<T> createBTree(Function<String, T> mapper, int index, List<List<String>> rows) {
        return createBTree(mapper, index, getDistinctSortedRows(mapper, index, rows), getIndexesWithColumnValue(mapper, index, rows));
    }

    private static <T extends Comparable<T>> List<List<String>> getDistinctSortedRows(Function<String, T> mapper, int index, List<List<String>> rows) {
        return sortByColumn(distinctBy(rows, row -> mapper.apply(row.get(index))), index, mapper);
    }

    private static <T extends Comparable<T>> Map<T, Set<Integer>> getIndexesWithColumnValue(Function<String, T> mapper,
                                                                                            int index,
                                                                                            List<List<String>> rows) {
        return CollectionsUtils.indexes(rows).collect(toMap(
                i -> mapper.apply(rows.get(i).get(index)),
                CollectionsUtils::singleSet,
                CollectionsUtils::merge
        ));
    }

    private static <T extends Comparable<T>> BTree<T> createBTree(final Function<String, T> mapper,
                                                                  final int index,
                                                                  List<List<String>> rows,
                                                                  final Map<T, Set<Integer>> indexes) {
        if (rows.isEmpty()) return null;
        if (rows.size() == 1) {
            T columnValue = mapper.apply(rows.get(0).get(index));
            BTree<T> root = new BTree<>(columnValue);
            root.getRowsIndexes().addAll(indexes.get(columnValue));
            return root;
        }

        int mid = rows.size() >>> 1;
        T midColumnValue = mapper.apply(rows.get(mid).get(index));
        BTree<T> root = new BTree<>(midColumnValue);
        root.getRowsIndexes().addAll(indexes.get(midColumnValue));
        root.setLeft(createBTree(mapper, index, rows.subList(0, mid), indexes));
        root.setRight(createBTree(mapper, index, rows.subList(mid + 1, rows.size()), indexes));
        return root;
    }

    private static int getIndex(String columnName, List<ColumnData> columns) {
        return CollectionsUtils.indexes(columns)
                .filter(i -> columns.get(i).first.equals(columnName))
                .findAny().orElseThrow(IllegalArgumentException::new);
    }
}
