package me.wbars.jdb.db;

import java.util.ArrayList;
import java.util.List;

public class BTree<T extends Comparable<T>> {
    private BTree<T> left;
    private BTree<T> right;
    private final T value;
    private final List<Integer> rowsIndexes = new ArrayList<>();

    public BTree(T value) {
        this.value = value;
    }

    public BTree<T> getLeft() {
        return left;
    }

    public void setLeft(BTree<T> left) {
        this.left = left;
    }

    public BTree<T> getRight() {
        return right;
    }

    public void setRight(BTree<T> right) {
        this.right = right;
    }

    public Comparable<T> getValue() {
        return value;
    }

    public List<Integer> getRowsIndexes() {
        return rowsIndexes;
    }

    public BTree<T> find(T value) {
        if (this.value == value) return this;
        if (this.value.compareTo(value) < 0) return left != null ? left.find(value) : null;
        return right != null ? right.find(value) : null;
    }

    public void insert(T value, int index) {
        if (this.value == value) {
            rowsIndexes.add(index);
            return;
        }
        if (value.compareTo(this.value) < 0) {
            if (left != null) left.insert(value, index);
            else {
                BTree<T> l = new BTree<>(value);
                l.rowsIndexes.add(index);
                left = l;
            }
            return;
        }

        if (right != null) right.insert(value, index);
        else {
            BTree<T> r = new BTree<>(value);
            r.rowsIndexes.add(index);
            right = r;
        }
    }

}
