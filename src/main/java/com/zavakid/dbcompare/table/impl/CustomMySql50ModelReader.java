package com.zavakid.dbcompare.table.impl;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.mysql.MySql50ModelReader;

/**
 * @author Zavakid 2012-12-12 下午5:59:36
 * @since 0.0.1
 */
public class CustomMySql50ModelReader extends MySql50ModelReader {

    /**
     * @param platform
     */
    public CustomMySql50ModelReader(Platform platform){
        super(platform);
    }

}
