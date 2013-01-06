package com.zavakid.dbcompare.table;

/***
 * @author Zavakid 2013-1-7 上午3:40:05
 * @since 0.0.1
 */
public interface DiffDescription {

    void diff();

    /**
     * must call diff() first
     * 
     * @return
     */
    boolean isDifferent();

    /**
     * must call diff() first
     * 
     * @return
     */
    String showResult();

    /**
     * must call diff() first
     * 
     * @return
     */
    String suggestSql();

}
