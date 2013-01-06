package com.zavakid.dbcompare.table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.zavakid.dbcompare.table.impl.DefaultMysqlTable;

/**
 * @author zebin.xuzb 2012-12-21 上午11:20:35
 * @since 4.1.3
 */
public class DefaultMysqlTableTest {

    private static Set<String>     COLUMNS = new HashSet<String>();
    private static BasicDataSource DATA_SOURCE;
    private static JdbcTemplate    JDBCTEMPLATE;

    @BeforeClass
    public static void init() {
        COLUMNS.add("ID");
        COLUMNS.add("ADDRESS");
        COLUMNS.add("EVENT_DATA");
        COLUMNS.add("PHONE");
        COLUMNS.add("CONTENT_CLOB");
        COLUMNS.add("CONTENT_BLOB");
        COLUMNS.add("STATUS");
        COLUMNS.add("NUMBER_VALUES");
        COLUMNS.add("REMARKS");
        COLUMNS.add("REMARKS2");
        COLUMNS.add("gmt_create");
        COLUMNS.add("gmt_modified");

        DATA_SOURCE = new BasicDataSource();
        DATA_SOURCE.setDriverClassName("com.mysql.jdbc.Driver");
        DATA_SOURCE.setUrl("jdbc:mysql://10.20.144.25:3306/otter1?autoReconnect=true&useUnicode=true&characterEncoding=utf8");
        DATA_SOURCE.setUsername("ottermysql");
        DATA_SOURCE.setPassword("ottermysql");

        JDBCTEMPLATE = new JdbcTemplate(DATA_SOURCE);
    }

    @AfterClass
    public static void destory() throws SQLException {
        DATA_SOURCE.close();
    }

    @Test
    public void test() {
        final Table table = new DefaultMysqlTable(JDBCTEMPLATE, "otter1", "ljhtable1");
        Assert.assertEquals("otter1", table.getSchemaName());
        Assert.assertEquals("ljhtable1", table.getTableName());
        Assert.assertEquals("otter1.ljhtable1", table.getFullName());
        Assert.assertEquals(COLUMNS.size(), table.getColumnNames().size());
        Assert.assertTrue(COLUMNS.containsAll(table.getColumnNames()));
        Assert.assertTrue(table.getColumnNames().containsAll(COLUMNS));

        int count = table.getCount();
        int countFound = JDBCTEMPLATE.queryForInt("select count(*) from ljhtable1");
        Assert.assertEquals(countFound, count);

        Assert.assertEquals(3, table.getPkNames().size());
        Assert.assertEquals("ID", table.getPkNames().get(0));

        final Map<String, String> finded = new HashMap<String, String>();
        JDBCTEMPLATE.query("select * from ljhtable1 where id = 1", new RowMapper() {

            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                for (String columnName : COLUMNS) {
                    String value = rs.getString(columnName);
                    finded.put(columnName, value);
                }

                return null;
            }
        });

        Map<String, Record> result = table.findRecord("where id = 1");
        for (Entry<String, Record> e : result.entrySet()) {
            Record record = e.getValue();
            for (String columnName : table.getColumnNames()) {
                String vauleFindFromTable = record.getValue(columnName);
                String valueFindFromJDBC = finded.get(columnName);
                Assert.assertEquals(valueFindFromJDBC, vauleFindFromTable);
            }
        }
    }
}
