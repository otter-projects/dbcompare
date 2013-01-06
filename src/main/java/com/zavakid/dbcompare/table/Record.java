package com.zavakid.dbcompare.table;

import java.util.Set;

/**
 * @author Zavakid 2013-1-7 上午3:40:11
 * @since 0.0.1
 */
public interface Record {

    String getValue(String name);

    String putValue(String name, String value);

    Table getTable();

    DiffDescription diff(Record another);

    Set<String> getColumnNames();

}
