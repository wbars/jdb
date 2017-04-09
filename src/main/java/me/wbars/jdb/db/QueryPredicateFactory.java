package me.wbars.jdb.db;

import me.wbars.jdb.query.QueryPredicate;
import me.wbars.jdb.scanner.Token;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static me.wbars.jdb.utils.CollectionsUtils.indexes;

public class QueryPredicateFactory {
    private static final Map<Type, BiFunction<String, Token, Predicate<String>>> typePredicates;

    static {
        typePredicates = new HashMap<>();
        typePredicates.put(Type.INTEGER, (operator, token) -> value -> {
            int valueToCompare = Integer.parseInt(token.value);
            int cell = Integer.parseInt(value);
            switch (operator) {
                case ">":
                    return cell > valueToCompare;
                case "<":
                    return cell < valueToCompare;
                case "=":
                    return cell == valueToCompare;
                case "<=":
                    return cell <= valueToCompare;
                case ">=":
                    return cell >= valueToCompare;
                case "!=":
                    return cell != valueToCompare;
                default:
                    throw new IllegalArgumentException(operator);
            }
        });

        typePredicates.put(Type.STRING, (operator, token) -> value -> {
            String valueToCompare = token.value;
            switch (operator) {
                case ">":
                    return value.compareTo(valueToCompare) > 0;
                case "<":
                    return value.compareTo(valueToCompare) < 0;
                case "=":
                    return Objects.equals(value, valueToCompare);
                case "<=":
                    return value.compareTo(valueToCompare) <= 0;
                case ">=":
                    return value.compareTo(valueToCompare) >= 0;
                case "!=":
                    return !Objects.equals(value, valueToCompare);
                default:
                    throw new IllegalArgumentException(operator);
            }
        });
    }

    private QueryPredicateFactory() {
    }

    public static QueryPredicate create(String columnName, String operator, Token value) {
        return (row, columns) -> {
            String cellValue = row.get(getIndex(columnName, columns));
            return typePredicates.get(getType(columnName, columns)).apply(operator, value).test(cellValue);
        };
    }

    private static Type getType(String columnName, List<ColumnData> columns) {
        return columns.stream()
                .filter(r -> r.first.equals(columnName))
                .map(r -> r.second).findAny()
                .orElseThrow(IllegalArgumentException::new);
    }

    private static int getIndex(String columnName, List<ColumnData> columns) {
        return indexes(columns)
                .filter(i -> columns.get(i).first.equals(columnName))
                .findAny().orElseThrow(IllegalArgumentException::new);
    }
}
