package com.zavakid.dbcompare.work;

/**
 * @author Zavakid 2013-1-7 上午3:41:14
 * @since 0.0.1
 */
public enum WorkMode {

    MANUAL, SMART;

    public boolean isManual() {
        return MANUAL.equals(this);
    }

    public boolean isAuto() {
        return SMART.equals(this);
    }
}
