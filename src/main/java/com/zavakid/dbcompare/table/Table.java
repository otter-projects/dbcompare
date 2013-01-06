package com.zavakid.dbcompare.table;

import java.util.List;
import java.util.Map;

/**
 * @author Zavakid 2013-1-7 上午3:40:18
 * @since 0.0.1
 */
public interface Table {

    /**
     * @return e.g. database_1.table_1
     */
    String getFullName();

    /**
     * @return e.g. database_1
     */
    String getSchemaName();

    /**
     * @return e.g. table_1
     */
    String getTableName();

    /**
     * @return the total records of the table
     */
    int getCount();

    /**
     * find record through whereCondition
     * 
     * @param whereCondition e.g. id in (1,2,3)
     * @return
     */
    Map<String, Record> findRecord(String whereCondition);

    /**
     * @param whereCondition e.g. id in (1,2,3)
     * @return
     */
    String buildSelectSql(String whereCondition);

    List<String> getColumnNames();

    List<String> getPkNames();

}
