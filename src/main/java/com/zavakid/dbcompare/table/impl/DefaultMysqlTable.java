package com.zavakid.dbcompare.table.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.zavakid.dbcompare.table.Record;
import com.zavakid.dbcompare.table.Table;

/**
 * @author Zavakid 2013-1-7 上午3:39:30
 * @since 0.0.1
 */
public class DefaultMysqlTable implements Table {

    private static final Logger             log       = LoggerFactory.getLogger(DefaultMysqlTable.class);

    private static final String             COUNT_SQL = "select count(*) from %s";

    private Database                        database;
    private org.apache.ddlutils.model.Table innerTable;
    private Column                          pkColumn;

    private JdbcTemplate                    jdbcTemplate;

    private String                          schemaName;
    private String                          tableName;
    private String                          fullName;

    public DefaultMysqlTable(JdbcTemplate jdbcTemplate, String schemaName, String tableName){
        this.jdbcTemplate = jdbcTemplate;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fullName = schemaName + "." + tableName;
        init();
    }

    protected void init() {
        database = DdlUtilsCache.getDatabase(jdbcTemplate.getDataSource(), schemaName);
        innerTable = database.findTable(this.getTableName());
        findPk();
    }

    protected void findPk() {
        Column[] pkColumns = innerTable.getPrimaryKeyColumns();
        if (pkColumns == null || pkColumns.length == 0) {
            throw new IllegalStateException(fullName + "has no pk column");
        }

        // first find the ID column
        for (Column c : pkColumns) {
            if (StringUtils.equalsIgnoreCase(c.getName(), "ID")) {
                pkColumn = c;
                break;
            }
        }
        // fallback to find a numberic column
        if (pkColumn == null) {
            for (Column c : pkColumns) {
                switch (c.getTypeCode()) {
                    case Types.BIGINT:
                    case Types.INTEGER:
                    case Types.NUMERIC:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        pkColumn = c;
                        break;

                    default:
                        break;
                }
            }
        }

        if (pkColumn == null) {
            // TODO need to find pk of varchar
            throw new IllegalStateException(fullName + "has no pk of numberic");
        }

    }

    @Override
    public int getCount() {
        String sql = String.format(COUNT_SQL, getFullName());
        int count = jdbcTemplate.queryForInt(sql);
        // log.info("find {} records from table {}", count, getTableName());
        return count;
    }

    @Override
    public Map<String, Record> findRecord(String whereCondition) {
        String sql = buildSelectSql(whereCondition);
        // log.info("start to findRecord from table : {} , sql : {}", getFullName(), sql);
        final Map<String, Record> result = new HashMap<String, Record>();

        jdbcTemplate.query(sql, new RowMapper() {

            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                Record record = new MappedRecord(DefaultMysqlTable.this);
                for (Column column : getComapringColumn()) {
                    String name = column.getName();
                    String value = rs.getString(name);
                    record.putValue(name, value);
                }

                String pkValue = rs.getString(pkColumn.getName());
                result.put(pkValue, record);
                return null;
            }

        });

        return result;
    }

    @Override
    public String buildSelectSql(String whereCondition) {
        StringBuilder sb = new StringBuilder("select ");

        List<String> columns = getColumnNames();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);

            if ((i + 1) == columns.size()) {
                sb.append(columnName).append(" ");
            } else {
                sb.append(columnName).append(" , ");
            }
        }

        sb.append("from ").append(getFullName()).append(" ");
        sb.append(whereCondition).append(";");
        return sb.toString();
    }

    @Override
    public String getFullName() {
        return this.fullName;
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public List<String> getColumnNames() {
        Column[] columns = getComapringColumn();
        List<String> result = new ArrayList<String>(columns.length);
        for (Column c : columns) {
            result.add(c.getName());
        }
        return result;
    }

    @Override
    public List<String> getPkNames() {
        Column[] columns = innerTable.getPrimaryKeyColumns();
        List<String> result = new ArrayList<String>(columns.length);
        for (Column c : columns) {
            result.add(c.getName());
        }
        return result;
    }

    /**
     * left for subclass to extend, default strategy : comparing all column
     * 
     * @return
     */
    protected Column[] getComapringColumn() {
        return innerTable.getColumns();
    }

    protected Column[] getAllColumn() {
        return innerTable.getColumns();
    }

    @Override
    public String getMainPkName() {
        return pkColumn.getName();
    }

}
