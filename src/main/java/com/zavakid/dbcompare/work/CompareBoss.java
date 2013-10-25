package com.zavakid.dbcompare.work;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

import com.zavakid.dbcompare.CompareLocator;

/**
 * 对应的对比线程任务分配者
 * 
 * @author jianghang 2012-2-20 下午09:10:31
 * @version 0.0.1
 */
public class CompareBoss implements InitializingBean, DisposableBean {

    private static final Logger logger    = LoggerFactory.getLogger(CompareBoss.class);
    // 注入的配置信息
    private int                 threads   = 5;
    private int                 batchSize = 50;
    private String              srcCountSql;
    private String              destCountSql;
    // 运行时需要的对象
    private JdbcTemplate        srcJdbcTemplate;
    private JdbcTemplate        destJdbcTemplate;
    private ExecutorService     executor  = null;
    private CompletionService   competion = null;

    @SuppressWarnings("unchecked")
    public void start() throws InterruptedException {
        int total = srcJdbcTemplate.queryForInt(srcCountSql);
        int destTotal = total;
        // int destTotal = destJdbcTemplate.queryForInt(destCountSql);
        logger.info("[total count: {} vs {}]", total, destTotal);

        int pages = total / batchSize + 1;
        int page = 0;
        for (; page < threads && page < pages; page++) { // 开始提交任务
            int start = page * batchSize;
            logger.info("[start : {} , end {}]", start, start + batchSize);
            competion.submit(buildWorker(start));
        }

        for (; page < pages; page++) {
            competion.take(); // 返回一个任务，继续下一个
            int start = page * batchSize;
            logger.info("[start : {} , end {}]", start, start + batchSize);
            competion.submit(buildWorker(start));
        }

        for (int i = 0; i < threads; i++) {
            competion.take(); // 收集完所有的任务
        }
    }

    public void afterPropertiesSet() throws Exception {
        validate();
        initExecutor();
        configureJdbcTemplate();
    }

    protected void validate() {
        Assert.notNull(srcCountSql);
    }

    protected void initExecutor() {
        if (executor == null) {
            executor = new ThreadPoolExecutor(threads, threads, 60 * 1000L, TimeUnit.MILLISECONDS,
                                              new LinkedBlockingQueue<Runnable>());
            competion = new ExecutorCompletionService(executor);

        }
    }

    protected void configureJdbcTemplate() {
        srcJdbcTemplate.setFetchSize(batchSize);
        destJdbcTemplate.setFetchSize(batchSize);
    }

    protected CompareWorker buildWorker(int start) {
        CompareWorker worker = CompareLocator.getCompareWoker();
        worker.setStart(start);
        worker.setCount(batchSize);
        return worker;
    }

    public void destroy() throws Exception {
        executor.shutdown();// 执行关闭
    }

    // ================= setter & getter ===================
    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setSrcJdbcTemplate(JdbcTemplate srcJdbcTemplate) {
        this.srcJdbcTemplate = srcJdbcTemplate;
    }

    public void setDestJdbcTemplate(JdbcTemplate destJdbcTemplate) {
        this.destJdbcTemplate = destJdbcTemplate;
    }

    public void setSrcCountSql(String srcCountSql) {
        this.srcCountSql = srcCountSql;
    }

    public void setDestCountSql(String destCountSql) {
        this.destCountSql = destCountSql;
    }

    public int getThreads() {
        return threads;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public JdbcTemplate getSrcJdbcTemplate() {
        return srcJdbcTemplate;
    }

    public JdbcTemplate getDestJdbcTemplate() {
        return destJdbcTemplate;
    }

    public CompletionService getCompetion() {
        return competion;
    }

}
