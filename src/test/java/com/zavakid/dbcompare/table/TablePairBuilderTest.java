package com.zavakid.dbcompare.table;

import com.zavakid.dbcompare.BaseTest;

/**
 * @author zebin.xuzb 2012-12-12 下午3:13:20
 * @since 4.1.3
 */
public class TablePairBuilderTest extends BaseTest {

    private static final String TABLE_1       = "otter1.table1";
    private static final String TABLE_2       = "otter2.table2";
    private static final String TABLE_3       = "otter3.table3";
    private static final String TABLE_4       = "otter4.table4";

    private static final String SRC_TABLES    = TABLE_1 + "," + TABLE_2;
    private static final String DEST_TABLES_1 = "";
    private static final String DEST_TABLES_2 = TABLE_3 + "," + TABLE_4;
    private static final String DEST_TABLES_3 = TABLE_3 + ",";

    // @Test
    // public void testSamePair() {
    // List<TablePairFactory> pairs = TablePairFactory.build(SRC_TABLES, DEST_TABLES_1);
    // Assert.assertEquals(2, pairs.size());
    // for (TablePairFactory pair : pairs) {
    // Assert.assertEquals(pair.getSrcTable(), pair.getDestTable());
    // }
    // Assert.assertEquals(TABLE_1, pairs.get(0).getSrcTable());
    // Assert.assertEquals(TABLE_2, pairs.get(1).getSrcTable());
    // }
    //
    // @Test
    // public void testDifferentPair() {
    // List<TablePairFactory> pairs = TablePairFactory.build(SRC_TABLES, DEST_TABLES_2);
    // Assert.assertEquals(2, pairs.size());
    //
    // Assert.assertEquals(TABLE_1, pairs.get(0).getSrcTable());
    // Assert.assertEquals(TABLE_3, pairs.get(0).getDestTable());
    //
    // Assert.assertEquals(TABLE_2, pairs.get(1).getSrcTable());
    // Assert.assertEquals(TABLE_4, pairs.get(1).getDestTable());
    // }
    //
    // @Test
    // public void testException() {
    // try {
    // TablePairFactory.build(SRC_TABLES, DEST_TABLES_3);
    // } catch (IllegalArgumentException e) {
    // return;
    // }
    // unReachable();
    // }
}
