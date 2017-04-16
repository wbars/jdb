package me.wbars.jdb.query;

public enum CompareSign {
    GT, LT, LTE, GTE, EQ, NE;

    public static CompareSign fromAlias(String operator) {
        if (operator.equals(">")) return GT;
        if (operator.equals("<")) return LT;
        if (operator.equals(">=")) return GTE;
        if (operator.equals("<=")) return LTE;
        if (operator.equals("=")) return EQ;
        if (operator.equals("!=")) return NE;
        throw new IllegalArgumentException();
    }
}
