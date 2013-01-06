package com.zavakid.dbcompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zavakid.dbcompare.work.CompareBoss;

/**
 * 启动一个对比程序
 * 
 * @author jianghang 2012-2-20 下午09:04:09
 * @version 4.0.0
 */
public class CompareLauncher {

    private static final Logger logger = LoggerFactory.getLogger(CompareLauncher.class);

    public static void main(String[] args) {
        logger.info("INFO ## load the config");
        CompareLocator.getApplicationContext();
        logger.info("INFO ## start compare ......");
        CompareBoss boss = CompareLocator.getCompareBoss();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                CompareLocator.close();
            }

        });

        try {
            boss.start();
        } catch (InterruptedException e) {
            // ignore
        }
        logger.info("INFO ## end compare");
        CompareLocator.close();
    }

}
