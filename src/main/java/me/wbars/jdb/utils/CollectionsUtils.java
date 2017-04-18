package me.wbars.jdb.utils;

import java.util.*;
import java.util.function.Function;
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

    public static <T, S> List<T> distinctBy(List<T> rows, Function<T, S> f) {
        return new ArrayList<>(rows.stream().collect(Collectors.toMap(f, t -> t)).values());
    }

    public static <T> Set<T> singleSet(T item) {
        Set<T> result = new HashSet<>();
        result.add(item);
        return result;
    }

    public static <T> Set<T> merge(Set<T> set, Set<T> set1) {
        Set<T> result = new HashSet<>();
        result.addAll(set);
        result.addAll(set1);
        return result;
    }

    public static <T, S> List<T> withoutNulls(List<T> l, Function<T, S> f) {
        return l.stream().filter(a -> f.apply(a) != null).collect(toList());
    }
}
