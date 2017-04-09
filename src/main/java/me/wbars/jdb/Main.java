package me.wbars.jdb;

import me.wbars.jdb.db.DatabaseService;
import me.wbars.jdb.db.QueryResult;
import me.wbars.jdb.table.Table;

import java.io.PrintStream;
import java.util.Scanner;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {
        DatabaseService service = new DatabaseService();
        Scanner scanner = new Scanner(System.in);
        PrintStream out = System.out;
        while (true) {
            String s = scanner.nextLine();
            if (s.equals("exit")) return;

            try {
                QueryResult result = service.executeQuery(s);
                out.println(result.isOk() ? "OK" : "FAIL");
                if (!result.isOk()) {
                    out.println(result.getMessage());
                    continue;
                }
                if (result.getTable() == null) continue;
                printTable(out, result.getTable());
            } catch (Exception e) {
                out.println("There was an exception: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void printTable(PrintStream out, Table table) {
        out.println("Table: " + table.getName());
        String header = concatValues(table.getColumns().stream().map(r -> r.first));
        out.println(header);
        out.println(lineOfLength(header.length()));
        table.getRows().forEach(row -> out.println(concatValues(row.stream())));
    }

    private static String lineOfLength(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) sb.append("-");
        return sb.toString();
    }

    private static String concatValues(Stream<String> row) {
        return row.reduce((s1, s2) -> s1 + "|" + s2).orElse("");
    }
}
