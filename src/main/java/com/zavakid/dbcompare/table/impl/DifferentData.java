package com.zavakid.dbcompare.table.impl;

import java.util.ArrayList;
import java.util.List;

import com.zavakid.dbcompare.common.Utils;

/**
 * @author Zavakid 2013-1-7 上午3:39:37
 * @since 0.0.1
 */
public class DifferentData {

    private String              fullTableName;
    private String              pkName;
    private String              pkValue;
    private List<DifferentItem> items = new ArrayList<DifferentItem>();

    public DifferentData(String fullTableName, String pkName, String pkValue){
        this.fullTableName = fullTableName;
        this.pkName = pkName;
        this.pkValue = pkValue;
    }

    public void addItem(String colName, String srcValue, String destValue) {
        items.add(new DifferentItem(colName, srcValue, destValue));
    }

    public List<String> getDifferentCols() {
        List<String> result = new ArrayList<String>(items.size());
        for (DifferentItem item : items) {
            result.add(item.getColName());
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table : ").append(fullTableName).append("  ").append(pkName).append(" : ").append(pkValue).append(Utils.SEP);
        sb.append("different data:").append(Utils.SEP);
        for (DifferentItem item : items) {
            sb.append(item.toString());
        }
        sb.append(Utils.SEP);
        return sb.toString();
    }

    private static class DifferentItem {

        private String colName;
        private String srcValue;
        private String destValue;

        public DifferentItem(String colName, String srcValue, String destValue){
            this.colName = colName;
            this.srcValue = srcValue;
            this.destValue = destValue;
        }

        public String getColName() {
            return colName;
        }

        public String getSrcValue() {
            return srcValue;
        }

        public String getDestValue() {
            return destValue;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(colName).append(" :").append(Utils.SEP);
            sb.append("  src : ").append(srcValue).append(Utils.SEP);
            sb.append("  dest: ").append(destValue).append(Utils.SEP);
            return sb.toString();
        }
    }
}
