package com.zavakid.dbcompare.table;

/**
 * @author Zavakid 2013-1-7 上午3:40:26
 * @since 0.0.1
 */
public class TablePair {

    private Table srcTable;
    private Table destTable;

    public TablePair(Table srcTable, Table destTable){
        this.srcTable = srcTable;
        this.destTable = destTable;
    }

    public Table getSrcTable() {
        return srcTable;
    }

    public Table getDestTable() {
        return destTable;
    }

}
