package me.wbars.jdb.db;

import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;
import me.wbars.jdb.table.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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

        List<List<String>> rows = table.getRows();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).get(0), is("test"));
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

        List<List<String>> rows = storage.selectAllRows("test");
        assertThat(rows, hasSize(3));
        rows.forEach(row -> assertThat(row, hasSize(2)));

        assertThat(rows.get(0).get(0), is("1"));
        assertThat(rows.get(0).get(1), is("a12"));

        assertThat(rows.get(1).get(0), is(nullValue()));
        assertThat(rows.get(1).get(1), is("bw"));

        assertThat(rows.get(2).get(0), is("3"));
        assertThat(rows.get(2).get(1), is(nullValue()));
    }

    @Test
    public void selectValues() throws Exception {
        createTestTable();
        databaseService.executeQuery("insert into `test`(`id`, `data`) values(1, `a12`)");
        databaseService.executeQuery("insert into `test`(`data`) values(`bb`)");

        QueryResult result = databaseService.executeQuery("select (`id`, `data`) from `test`");
        assertThat(result.isOk(), is(true));

        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(2));
        rows.forEach(row -> assertThat(row, hasSize(2)));

        assertThat(rows.get(0).get(0), is("1"));
        assertThat(rows.get(0).get(1), is("a12"));

        assertThat(rows.get(1).get(0), is(nullValue()));
        assertThat(rows.get(1).get(1), is("bb"));
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

        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(2));
        rows.forEach(row -> assertThat(row, hasSize(1)));
        assertThat(rows.get(0).get(0), is("a12"));
        assertThat(rows.get(1).get(0), is("bb"));

        assertThat(result.getTable().getColumns(), hasSize(1));
        assertThat(result.getTable().getColumns().get(0).first, is("data"));
        assertThat(result.getTable().getColumns().get(0).second, is(Type.STRING));
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
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(2));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));

        assertThat(result.getTable().getColumns(), hasSize(1));
        assertThat(result.getTable().getColumns().get(0).first, is("data"));
        assertThat(result.getTable().getColumns().get(0).second, is(Type.STRING));
    }

    @Test
    public void selectWithPredicateGT() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` > 1");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(1));

        assertThat(rows.get(0).get(0), is("two"));
    }

    @Test
    public void selectWithPredicateLT() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 2");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(2));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));
    }

    @Test
    public void selectWithPredicateLTE() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` <= 2");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(3));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));
        assertThat(rows.get(2).get(0), is("two"));
    }

    @Test
    public void selectWithPredicateGTE() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` >= 1");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(3));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));
        assertThat(rows.get(2).get(0), is("two"));
    }

    @Test
    public void selectWithPredicateNe() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` != 2");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(2));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));
    }

    @Test
    public void selectWithPredicateAND() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` <= 1 and `data` != `one`");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(1));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one_again"));
    }

    @Test
    public void selectWithPredicateOR() throws Exception {
        createTestTable();
        populateDummyData();

        QueryResult result = databaseService.executeQuery("select (`data`) from `test` where `id` < 2 or `data` = `two`");
        assertThat(result.isOk(), is(true));
        List<List<String>> rows = result.getTable().getRows();
        assertThat(rows, hasSize(3));
        rows.forEach(row -> assertThat(row, hasSize(1)));

        assertThat(rows.get(0).get(0), is("one"));
        assertThat(rows.get(1).get(0), is("one_again"));
        assertThat(rows.get(2).get(0), is("two"));
    }
}