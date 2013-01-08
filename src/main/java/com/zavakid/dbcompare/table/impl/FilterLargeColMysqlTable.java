package com.zavakid.dbcompare.table.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.ddlutils.model.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author Zavakid 2013-1-7 上午3:39:48
 * @since 0.0.1
 */
public class FilterLargeColMysqlTable extends DefaultMysqlTable {

    private static final Logger log                     = LoggerFactory.getLogger(DefaultMysqlTable.class);

    private int                 maxAcceptSize           = 256;
    private Object              getComparingColumnsLock = new Object();
    private Column[]            comparingColumnArray    = null;

    public FilterLargeColMysqlTable(JdbcTemplate jdbcTemplate, String schemaName, String tableName){
        super(jdbcTemplate, schemaName, tableName);
    }

    @Override
    protected Column[] getComapringColumn() {

        if (comparingColumnArray != null) {
            return comparingColumnArray;
        }

        synchronized (getComparingColumnsLock) {
            if (comparingColumnArray != null) {
                return comparingColumnArray;
            }
            comparingColumnArray = doGetCompareColumns();
            return comparingColumnArray;

        }
    }

    protected Column[] doGetCompareColumns() {
        Column[] all = getAllColumn();
        List<Column> comparingColumns = new ArrayList<Column>();
        for (Column col : all) {
            // filter bin field and special
            if (col.isOfBinaryType() || col.isOfSpecialType()) {
                log.warn(String.format("%s skip to compare %s because the column is binaryType or specialType, the column is : %s",
                                       getFullName(),
                                       ToStringBuilder.reflectionToString(col, ToStringStyle.MULTI_LINE_STYLE)));
                continue;
            }

            // if column char size > maxAcceptSize, then filter
            if (col.isOfTextType() && col.getSizeAsInt() > getMaxAcceptSize()) {
                log.warn(String.format("%s skip to compare %s because the column size it over %s , the column is %s",
                                       getFullName(), col.getName(), getMaxAcceptSize(),
                                       ToStringBuilder.reflectionToString(col, ToStringStyle.MULTI_LINE_STYLE)));
                continue;
            }

            comparingColumns.add(col);
        }
        return comparingColumns.toArray(new Column[comparingColumns.size()]);
    }

    public int getMaxAcceptSize() {
        return maxAcceptSize;
    }

    public void setMaxAcceptSize(int maxAcceptSize) {
        this.maxAcceptSize = maxAcceptSize;
    }

}
