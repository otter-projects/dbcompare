package com.zavakid.dbcompare;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.zavakid.dbcompare.work.CompareBoss;
import com.zavakid.dbcompare.work.CompareWorker;

/**
 * compare spring容器获取
 * 
 * @author jianghang 2012-2-20 下午09:05:59
 * @version 4.0.0
 */
public class CompareLocator {

    private static ApplicationContext context       = null;
    private static RuntimeException   initException = null;

    static {
        try {
            context = new ClassPathXmlApplicationContext("compare.xml");
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public static ApplicationContext getApplicationContext() {
        if (context == null) {
            throw initException;
        }

        return context;
    }

    public static void close() {
        ((ClassPathXmlApplicationContext) context).close();
    }

    public static CompareBoss getCompareBoss() {
        return (CompareBoss) getApplicationContext().getBean("smartCompareBoss");
    }

    public static CompareWorker getCompareWoker() {
        return (CompareWorker) getApplicationContext().getBean("compareWorker");
    }
}
