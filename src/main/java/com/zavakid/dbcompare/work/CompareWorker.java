package com.zavakid.dbcompare.work;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * 具体的对比工作者
 * 
 * @author jianghang 2012-2-20 下午09:10:10
 * @version 0.0.1
 */
public class CompareWorker implements Callable<Exception> {

    private static final Logger logger        = LoggerFactory.getLogger(CompareWorker.class);
    private static final String SEP           = SystemUtils.LINE_SEPARATOR;
    private static final String ORACLE        = "oracle";
    private String              pkName;
    private String              columnName;
    static String               failed_format = null;
    static String               row_format    = null;
    static {
        failed_format = SEP + "****************************************************" + SEP;
        failed_format += "* start : {0} , end : {1} , count {2}" + SEP;
        failed_format += "* successed : {3} , failed {4}" + SEP;
        failed_format += "{5}" + SEP;
        failed_format += "****************************************************" + SEP;

        row_format = "KEY: {0}" + SEP;
        row_format += "\t {1}" + SEP;
        row_format += "\t {2}" + SEP;
    }
    private int                 start;
    private int                 count;
    private String              fetchSql;
    private String              inSql;
    private JdbcTemplate        srcJdbcTemplate;
    private JdbcTemplate        destJdbcTemplate;

    public Exception call() throws Exception {
        int successed = 0;
        int failed = 0;
        Map<String, String> src = new HashMap<String, String>();
        Map<String, String> dest = new HashMap<String, String>();
        try {
            if (isOracle(srcJdbcTemplate)) { // oracle的话end,start顺序
                srcJdbcTemplate.query(formatFetchSql(start + count, start), new CompareRowMapper(src));
            } else {
                srcJdbcTemplate.query(formatFetchSql(start, count), new CompareRowMapper(src));
            }

            destJdbcTemplate.query(StringUtils.replace(inSql, "$1", StringUtils.join(src.keySet(), ',')),
                                   new CompareRowMapper(dest));
        } catch (Exception e) {
            logger.error(MessageFormat.format(failed_format, start, start + count, count, successed, failed, ""), e);
            return e;
        } finally {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, String> entry : src.entrySet()) {
                String key = entry.getKey();
                String srcValue = entry.getValue();
                String destValue = dest.get(key);
                if (StringUtils.equals(srcValue, destValue) == false) {
                    failed++;
                    builder.append(MessageFormat.format(row_format, key, srcValue, destValue));
                } else {
                    successed++;
                }
            }

            if (failed > 0) {
                logger.warn(MessageFormat.format(failed_format, start, start + count, count, successed, failed,
                                                 builder.toString()));
            } else {
                logger.info(MessageFormat.format(failed_format, start, start + count, count, successed, failed,
                                                 builder.toString()));
            }

        }
        return null;
    }

    private String formatFetchSql(int a, int b) {
        String result = StringUtils.replace(fetchSql, "$1", String.valueOf(a));
        return StringUtils.replace(result, "$2", "" + String.valueOf(b));
    }

    private boolean isOracle(JdbcTemplate jdbcTemplate) {
        return (Boolean) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                String databaseName = meta.getDatabaseProductName();
                if (StringUtils.startsWithIgnoreCase(databaseName, ORACLE)) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    class CompareRowMapper implements RowMapper {

        private Map data;

        public CompareRowMapper(Map data){
            this.data = data;
        }

        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            String pk = rs.getString(pkName);
            String column = rs.getString(columnName);
            data.put(pk, column);
            return null;
        }

    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setPkName(String pkName) {
        this.pkName = pkName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setFetchSql(String fetchSql) {
        this.fetchSql = fetchSql;
    }

    public void setInSql(String inSql) {
        this.inSql = inSql;
    }

    public void setSrcJdbcTemplate(JdbcTemplate srcJdbcTemplate) {
        this.srcJdbcTemplate = srcJdbcTemplate;
    }

    public void setDestJdbcTemplate(JdbcTemplate destJdbcTemplate) {
        this.destJdbcTemplate = destJdbcTemplate;
    }

}
