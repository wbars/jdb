package me.wbars.jdb.db;

import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class DatabaseServiceTest {
    private DatabaseService databaseService;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = new Storage();
        databaseService = new DatabaseService(new QueryParser(), storage);
    }

    @Test
    public void showTablesEmptyDatabase() throws Exception {
        QueryResult result = databaseService.executeQuery("show tables");

        assertThat(result.isOk(), is(true));
        List<ColumnData> columns = result.getTable().getColumns();
        assertThat(columns, hasSize(1));
        assertThat(columns.get(0).first, is("table"));
        assertThat(columns.get(0).second, is(Type.STRING));
        assertThat(result.getTable().getRows(), is(empty()));
    }

    @Test
    public void testCreateEmptyTable() throws Exception {
        QueryResult createTable = databaseService.executeQuery("create table `test`");
        assertThat(createTable.isOk(), is(true));
        assertThat(createTable.getTable(), is(nullValue()));

        QueryResult result = databaseService.executeQuery("show tables");

        assertThat(result.isOk(), is(true));

        Table table = result.getTable();
        List<ColumnData> columns = table.getColumns();
        assertThat(columns, hasSize(1));
        assertThat(columns.get(0).first, is("table"));
        assertThat(columns.get(0).second, is(Type.STRING));

        assertTableWithSizeAndValues(1, 1, singletonList(singletonList("test")), result.getTable());
    }

    @Test
    public void createTableWithExistingName() throws Exception {
        QueryResult createTable = databaseService.executeQuery("create table `test`");
        assertThat(createTable.isOk(), is(true));
        assertThat(createTable.getTable(), is(nullValue()));

        QueryResult createTableExistingName = databaseService.executeQuery("create table `test`");
        assertThat(createTableExistingName.isOk(), is(false));
        assertThat(createTableExistingName.getMessage(), is("Table `test` already exists"));
    }

    @Test
    public void dropTable() throws Exception {
        databaseService.executeQuery("create table `test`");
        databaseService.executeQuery("drop table `test`");

        QueryResult result = databaseService.executeQuery("show tables");

        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, is(empty()));
    }

    @Test
    public void deleteNonExistingTable() throws Exception {
        QueryResult result = databaseService.executeQuery("drop table `test`");
        assertThat(result.isOk(), is(false));
        assertThat(result.getMessage(), is("Table `test` not exists"));
    }

    @Test
    public void describeNonExistingTable() throws Exception {
        QueryResult result = databaseService.executeQuery("describe table `test`");
        assertThat(result.isOk(), is(false));
        assertThat(result.getMessage(), is("Table `test` not exists"));
    }

    @Test
    public void describeTable() throws Exception {
        createTestTable();
        QueryResult result = databaseService.executeQuery("describe table `test`");
        assertThat(result.isOk(), is(true));
        List<ColumnData> columns = result.getTable().getColumns();
        assertThat(columns, hasSize(2));

        assertThat(columns.get(0).first, is("id"));
        assertThat(columns.get(0).second, is(Type.INTEGER));

        assertThat(columns.get(1).first, is("data"));
        assertThat(columns.get(1).second, is(Type.STRING));
    }

    @Test
    public void insertValues() throws Exception {
        createTestTable();
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `a12`)");
        databaseService.executeQuery("insert into `test`(`data`) values(`bw`)");
        databaseService.executeQuery("insert into `test`(`id`) values(3)");
        assertTableWithSizeAndValues(3, 2, asList(asList("1", "a12"), asList(null, "bw"), asList("3", null)), storage.selectAllRows("test"));
    }

    @Test
    public void selectValues() throws Exception {
        createTestTable();
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `a12`)");
        databaseService.executeQuery("insert into `test`(`data`) values(`bb`)");

        QueryResult result = databaseService.executeQuery("select (`id`, `data`) from `test`");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(2, 2, asList(asList("1", "a12"), asList(null, "bb")), result.getTable());
    }

    private void createTestTable() {
        databaseService.executeQuery("create table `test` (" +
                "`id` integer," +
                "`data` string" +
                ")");
    }

    @Test
    public void selectNotAllColumns() throws Exception {
        createTestTable();
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `a12`)");
        databaseService.executeQuery("insert into `test`(`data`) values(`bb`)");

        QueryResult result = databaseService.executeQuery("select (`data`) from `test`");
        assertThat(result.isOk(), is(true));

        assertThat(result.getTable().getColumns(), hasSize(1));
        assertThat(result.getTable().getColumns().get(0).first, is("data"));
        assertThat(result.getTable().getColumns().get(0).second, is(Type.STRING));

        assertTableWithSizeAndValues(2, 1, asList(singletonList("a12"), singletonList("bb")), result.getTable());
    }

    private void populateDummyData() {
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `one`)");
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `one_again`)");
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(2, `two`)");
    }

    @Test
    public void selectWithPredicateEq() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` = 1");
        assertThat(result.isOk(), is(true));
        assertThat(result.getTable().getColumns(), hasSize(1));
        assertThat(result.getTable().getColumns().get(0).first, is("data"));
        assertThat(result.getTable().getColumns().get(0).second, is(Type.STRING));

        assertTableWithSizeAndValues(2, 1, asList(singletonList("one"), singletonList("one_again")), result.getTable());
    }

    @Test
    public void selectWithPredicateGT() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` > 1");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(1, 1, singletonList(singletonList("two")), result.getTable());
    }

    @Test
    public void selectWithPredicateLT() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 2");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(2, 1, asList(singletonList("one"), singletonList("one_again")), result.getTable());
    }

    @Test
    public void selectWithPredicateLTE() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` <= 2");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(3, 1, asList(singletonList("one"), singletonList("one_again"), singletonList("two")), result.getTable());
    }

    @Test
    public void selectWithPredicateGTE() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` >= 1");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(3, 1, asList(singletonList("one"), singletonList("one_again"), singletonList("two")), result.getTable());
    }

    @Test
    public void selectWithPredicateNe() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` != 2");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(2, 1, asList(singletonList("one"), singletonList("one_again")), result.getTable());
    }

    @Test
    public void selectWithPredicateAND() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` <= 1 and `data` != `one`");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(1, 1, singletonList(singletonList("one_again")), result.getTable());
    }

    @Test
    public void selectWithPredicateOR() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 2 or `data` = `two`");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(3, 1, asList(singletonList("one"), singletonList("one_again"), singletonList("two")), result.getTable());
    }

    @Test
    public void selectWithPredicateOrder() throws Exception {
        createTestTable();
        tenSampleRows();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` > 3 and `data` = `4` or `id` < 3");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(3, 1, asList(singletonList("1"), singletonList("2"), singletonList("4")), result.getTable());
    }

    @Test
    public void selectWithPredicateReverseOrder() throws Exception {
        createTestTable();
        tenSampleRows();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 3 or `data` = `4` and `id` > 3");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(3, 1, asList(singletonList("1"), singletonList("2"), singletonList("4")), result.getTable());
    }

    @Test
    public void selectWithPredicateDualOrder() throws Exception {
        createTestTable();
        tenSampleRows();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 3 and `data` = `2` or `id` > 5 and `data` = `6`");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(2, 1, asList(singletonList("2"), singletonList("6")), result.getTable());
    }

    private void assertTableWithSizeAndValues(int height, int width, List<List<String>> values, Table table) {
        assertTableWithSizeAndValues(height, width, values, table.getRows());
    }

    private void assertTableWithSizeAndValues(int height, int width, List<List<String>> values, List<List<String>> rows) {
        assertThat(rows, hasSize(height));
        rows.forEach(row -> assertThat(row, hasSize(width)));
        assertThat(rows, is(values));
    }

    private void tenSampleRows() {
        for (int i = 1; i < 10; i++)
            databaseService.executeQuery(String.format("insert into `test`(`id`, `data`) values(%d, `%s`)", i, String.valueOf(i)));
    }

    @Test
    public void selectWithPredicateParens() throws Exception {
        createTestTable();
        tenSampleRows();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where (`id` < 5 or `data` = `4`) and (`id` > 3)");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(1, 1, singletonList(singletonList("4")), result.getTable());
    }

    @Test
    public void selectWithPredicateInnerParens() throws Exception {
        createTestTable();
        tenSampleRows();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where ( (`id` < 5 or `data` = `4`) and (`id` > 3) )");
        assertThat(result.isOk(), is(true));
        assertTableWithSizeAndValues(1, 1, singletonList(singletonList("4")), result.getTable());
    }
}