package com.zavakid.dbcompare.table.impl;

import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.zavakid.dbcompare.common.Utils;
import com.zavakid.dbcompare.table.DiffDescription;
import com.zavakid.dbcompare.table.Record;

/**
 * No-Thread-Safe
 * 
 * @author Zavakid 2012-12-21 下午12:44:17
 * @since 0.0.1
 */
public class DefaultDiffDexcription implements DiffDescription {

    private static String BOTH_NULL_LOG        = Utils.SPLIT + "[OK], both null" + Utils.SPLIT;
    private static String MISSING_SOURE_LOG    = Utils.SPLIT
                                                 + "[WRONG : SOURCE NULL] %s  miss source recored, the destination recored is:"
                                                 + Utils.SEP + "%s" + Utils.SPLIT;
    private static String MISSING_DEST_LOG     = Utils.SPLIT
                                                 + "[WRONG : DEST NULL] %s  miss destination recored, the source recored is:"
                                                 + Utils.SEP + "%s" + Utils.SPLIT;

    private static String DIFFERENT_COLS_LOG   = Utils.SPLIT
                                                 + "[WRONG : DIFFERENT COLS] %s have different columns (columns name not same) :"
                                                 + Utils.SEP + "source : " + Utils.SEP + "%s" + Utils.SEP + "destion :"
                                                 + Utils.SEP + "%s" + Utils.SPLIT;

    private static String DIFFERENT_VALUES_LOG = Utils.SPLIT
                                                 + "[WRONG : DIFFERENT VALUES] %s have different values, different columns [%s] and data is :"
                                                 + Utils.SEP + "%s" + Utils.SPLIT;

    private Record        source;
    private Record        destination;

    private DifferentType type;
    private String        resultText;
    private DifferentData differentData;

    public DefaultDiffDexcription(Record source, Record destination){
        this.source = source;
        this.destination = destination;
    }

    @Override
    public void diff() {
        if (source == null && destination == null) {
            type = DifferentType.EQUALS;
            resultText = BOTH_NULL_LOG;
            return;
        }

        if (source == null) {
            type = DifferentType.MISS;
            resultText = String.format(MISSING_SOURE_LOG, destination.getTable().getFullName(), destination);
            return;
        }

        if (destination == null) {
            type = DifferentType.MISS;
            resultText = String.format(MISSING_DEST_LOG, source.getTable().getFullName(), source);
            return;
        }

        Set<String> sourceColumns = source.getColumnNames();
        Set<String> destColumns = destination.getColumnNames();

        // compare the columns
        if (!sourceColumns.equals(destColumns)) {
            type = DifferentType.DIFFERENT_COLUMNS;
            resultText = String.format(DIFFERENT_COLS_LOG, source.getTable().getFullName(), source, destination);
            return;
        }

        // compare the values
        boolean equals = true;
        for (String srcCol : sourceColumns) {
            String srcValue = source.getValue(srcCol);
            String destValue = destination.getValue(srcCol);

            // find different
            if (!StringUtils.equals(srcValue, destValue)) {
                equals = false;
                setDifferentDataIfNull();
                differentData.addItem(srcCol, srcValue, destValue);
            }
        }

        if (!equals) {
            type = DifferentType.DIFFERENT_DATA;

            String differentCols = StringUtils.join(differentData.getDifferentCols(), ",");
            resultText = String.format(DIFFERENT_VALUES_LOG, source.getTable().getFullName(), differentCols,
                                       differentData);

            return;
        }

        type = DifferentType.EQUALS;
        resultText = "OK";
    }

    private void setDifferentDataIfNull() {
        if (differentData != null) {
            return;
        }

        String fullTableName = source.getTable().getFullName();
        String firstPkColumnName = source.getTable().getMainPkName();
        String firstPkValue = source.getValue(firstPkColumnName);
        differentData = new DifferentData(fullTableName, firstPkColumnName, firstPkValue);
    }

    @Override
    public boolean isDifferent() {
        return !DifferentType.EQUALS.equals(type);
    }

    @Override
    public String showResult() {
        return resultText;
    }

    @Override
    public String suggestSql() {
        throw new UnsupportedOperationException("currently not supported");
    }

}
