package me.wbars.jdb.utils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class CollectionsUtils {
    public static <T> List<T> concat(T head, List<T> c) {
        ArrayList<T> result = new ArrayList<>();
        result.add(head);
        result.addAll(c);
        return result;
    }

    public static <K, V> Map<K, V> toMap(List<K> keys, List<V> values) {
        if (keys.size() != values.size()) throw new IllegalArgumentException();
        return range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }

    public static <T> List<T> withIndexes(List<T> l, Collection<Integer> indexesToRemain) {
        return indexes(l)
                .filter(indexesToRemain::contains)
                .map(l::get)
                .collect(toList());
    }

    public static <T> Stream<Integer> indexes(List<T> l) {
        return range(0, l.size()).boxed();
    }

    public static <T> Stream<Integer> indexes(T[] prefixTypes) {
        return range(0, prefixTypes.length).boxed();
    }

    public static <T> List<List<T>> wrapInLists(List<T> l) {
        return l.stream().map(Collections::singletonList).collect(toList());
    }
}
