package me.wbars.jdb.query;

import me.wbars.jdb.db.QueryParser;
import me.wbars.jdb.scanner.Type;
import me.wbars.jdb.table.ColumnData;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public class QueryParserTest {
    private QueryParser parser;

    @Before
    public void setUp() throws Exception {
        parser = new QueryParser();
    }

    @Test
    public void parseShowTables() throws Exception {
        Query query = parser.parse("show tables");
        assertThat(query, is(instanceOf(ShowTablesQuery.class)));
    }

    @Test
    public void parseCreateTable() throws Exception {
        Query query = parser.parse("create table `test`");
        assertThat(query, is(instanceOf(CreateTableQuery.class)));
        assertThat(((CreateTableQuery) query).getTableName(), is("test"));

        Query queryAbc = parser.parse("create table `abc`");
        assertThat(queryAbc, is(instanceOf(CreateTableQuery.class)));
        assertThat(((CreateTableQuery) queryAbc).getTableName(), is("abc"));
    }

    @Test
    public void parseDropTable() throws Exception {
        Query query = parser.parse("drop table `test`");
        assertThat(query, is(instanceOf(DropTableQuery.class)));
        assertThat(((DropTableQuery) query).getTableName(), is("test"));
    }

    @Test
    public void describeTable() throws Exception {
        Query query = parser.parse("describe table `test`");
        assertThat(query, is(instanceOf(DescribeTableQuery.class)));
        assertThat(((DescribeTableQuery) query).getTableName(), is("test"));
    }

    @Test
    public void parseCreateTableWithData() throws Exception {
        Query query = parser.parse("create table `test` (" +
                "`id` integer," +
                "`data` string" +
                ")");
        assertThat(query, is(instanceOf(CreateTableQuery.class)));
        assertThat(((CreateTableQuery) query).getTableName(), is("test"));

        List<ColumnData> columns = ((CreateTableQuery) query).getColumns();
        assertThat(columns, hasSize(2));
        assertThat(columns.get(0).first, is("id"));
        assertThat(columns.get(0).second, is(Type.INTEGER));

        assertThat(columns.get(1).first, is("data"));
        assertThat(columns.get(1).second, is(Type.STRING));
    }

    @Test
    public void selectFrom() throws Exception {
        Query query = parser.parse("select (`id`, `data`) from `test`");
        assertThat(query, is(instanceOf(SelectQuery.class)));
        assertThat(((SelectQuery) query).getTableName(), is("test"));

        List<String> columns = ((SelectQuery) query).getColumns();
        assertThat(columns, hasSize(2));
        assertThat(columns.get(0), is("id"));
        assertThat(columns.get(1), is("data"));
        assertThat(((SelectQuery) query).getPredicate(), is((nullValue())));
    }

    @Test
    public void selectFromWhere() throws Exception {
        Query query = parser.parse("select (`data`) from `test` where `id` = 1");
        assertThat(query, is(instanceOf(SelectQuery.class)));
        assertThat(((SelectQuery) query).getTableName(), is("test"));

        List<String> columns = ((SelectQuery) query).getColumns();
        assertThat(columns, hasSize(1));
        assertThat(columns.get(0), is("data"));

        assertThat(((SelectQuery) query).getPredicate(), is(notNullValue()));
    }

    @Test
    public void createIndexParser() throws Exception {
        Query query = parser.parse("create index `id` on `test`");
        assertThat(query, is(instanceOf(CreateIndexQuery.class)));
        assertThat(((CreateIndexQuery) query).getTableName(), is("test"));
        assertThat(((CreateIndexQuery) query).getColumn(), is("id"));
    }
}