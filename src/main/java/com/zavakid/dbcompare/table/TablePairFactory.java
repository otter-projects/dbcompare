package com.zavakid.dbcompare.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.zavakid.dbcompare.table.impl.FilterLargeColMysqlTable;

/**
 * @author Zavakid 2013-1-7 上午3:40:31
 * @since 0.0.1
 */
public class TablePairFactory {

    private static final String TABLE_SPLIT = ",";

    private String              srcTable;
    private String              destTable;

    private TablePairFactory(String srcTable, String destTable){
        this.srcTable = srcTable;
        this.destTable = destTable;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getDestTable() {
        return destTable;
    }

    // =========== static methods ============
    public static List<TablePair> build(String srcTables, JdbcTemplate srcJDBC, String destTables, JdbcTemplate destJDBC) {
        List<String> srcTableList = splitTable(srcTables);
        List<String> destTablesList = null;

        if (StringUtils.isNotBlank(destTables)) {
            destTablesList = splitTable(destTables);
            Assert.isTrue(srcTableList.size() == destTablesList.size());
        }

        if (srcTableList.size() == 0) {
            return Collections.emptyList();
        }

        if (CollectionUtils.isEmpty(destTablesList)) {
            return buildPairs(srcTableList, srcJDBC, srcTableList, destJDBC);
        } else {
            return buildPairs(srcTableList, srcJDBC, destTablesList, destJDBC);
        }
    }

    protected static List<TablePair> buildPairs(List<String> srcTableList, JdbcTemplate srcJDBC,
                                                List<String> destTablesList, JdbcTemplate destJDBC) {
        List<TablePair> results = new ArrayList<TablePair>();
        for (int i = 0; i < srcTableList.size(); i++) {
            Table srcTable = buildTable(srcTableList.get(i), srcJDBC);
            Table destTable = buildTable(destTablesList.get(i), destJDBC);
            TablePair pair = new TablePair(srcTable, destTable);
            results.add(pair);
        }
        return results;
    }

    private static List<String> splitTable(String tables) {
        if (StringUtils.isBlank(tables)) {
            return Collections.emptyList();
        }
        String[] temp = StringUtils.split(tables, TABLE_SPLIT);
        List<String> result = new ArrayList<String>();
        for (String table : temp) {
            table = StringUtils.trim(table);
            if (StringUtils.isBlank(table)) {
                continue;
            }
            result.add(table);
        }
        return result;
    }

    private static Table buildTable(String fullName, JdbcTemplate jdbcTemplate) {
        String[] scheamAndName = splitTableScheamAndName(fullName);
        return new FilterLargeColMysqlTable(jdbcTemplate, scheamAndName[0], scheamAndName[1]);
    }

    private static String[] splitTableScheamAndName(String fullName) {
        Assert.hasText(fullName);
        String[] scheamAndName = fullName.split("\\.");
        Assert.isTrue(2 == scheamAndName.length, "fullName is " + fullName);
        return scheamAndName;
    }
}
