package com.zavakid.dbcompare.work;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.zavakid.dbcompare.common.Utils;
import com.zavakid.dbcompare.table.Table;
import com.zavakid.dbcompare.table.TablePair;
import com.zavakid.dbcompare.table.TablePairFactory;

/**
 * @author Zavakid 2013-1-7 上午3:40:56
 * @since 0.0.1
 */
public class SmartCompareBoss extends CompareBoss {

    private static final Logger log = LoggerFactory.getLogger(SmartCompareBoss.class);

    private WorkMode            workMode;
    private String              srcTables;
    private String              destTables;

    private List<TablePair>     pairs;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        pairs = TablePairFactory.build(srcTables, getSrcJdbcTemplate(), destTables, getDestJdbcTemplate());
    }

    @Override
    protected void validate() {
        Assert.notNull(getSrcJdbcTemplate());
        Assert.notNull(getDestJdbcTemplate());
        Assert.hasText(getSrcTables());
    }

    @Override
    public void start() throws InterruptedException {
        if (workMode.isManual()) {
            super.start();
            return;
        }

        smartStart();
    }

    protected void smartStart() throws InterruptedException {
        for (TablePair pair : pairs) {
            MDC.put(Utils.LOG_SPLIT_KEY, pair.getSrcTable().getFullName());
            try {
                comparePair(pair);
            } finally {
                MDC.remove(Utils.LOG_SPLIT_KEY);
            }
        }
    }

    protected void comparePair(TablePair pair) throws InterruptedException {
        Table srcTable = pair.getSrcTable();
        Table destTable = pair.getDestTable();

        int srcCount = srcTable.getCount();
        log.info(Utils.SPLIT + "find {} from source table {}" + Utils.SPLIT, srcCount, srcTable.getFullName());

        int destCount = destTable.getCount();
        log.info(Utils.SPLIT + "find {} from dest table {}" + Utils.SPLIT, destCount, destTable.getFullName());

        int pages = srcCount / getBatchSize();
        if (srcCount % getBatchSize() != 0) {
            pages++;
        }
        int page = 0;
        for (; page < getThreads() && page < pages; page++) { // 开始提交任务
            int start = page * getBatchSize();
            log.info(Utils.SPLIT + "[start : {} , end {}]" + Utils.SPLIT, start, start + getBatchSize());
            getCompetion().submit(buildSmartWorker(srcTable, destTable, start, getBatchSize()));
        }

        for (; page < pages; page++) {
            getCompetion().take(); // 返回一个任务，继续下一个
            int start = page * getBatchSize();
            log.info(Utils.SPLIT + "[start : {} , end {}]" + Utils.SPLIT, start, start + getBatchSize());
            getCompetion().submit(buildSmartWorker(srcTable, destTable, start, getBatchSize()));
        }

        int workers = pages < getThreads() ? pages : getThreads();
        for (int i = 0; i < workers; i++) {
            getCompetion().take(); // 收集完所有的任务
        }

    }

    protected SmartCompareWorker buildSmartWorker(Table srcTable, Table destTable, int start, int size) {
        return new SmartCompareWorker(srcTable, destTable, start, size);
    }

    // ============= setter & getter ============
    public WorkMode getWorkMode() {
        return workMode;
    }

    public void setWorkMode(WorkMode workMode) {
        this.workMode = workMode;
    }

    public String getSrcTables() {
        return srcTables;
    }

    public String getDestTables() {
        return destTables;
    }

    public void setSrcTables(String srcTables) {
        this.srcTables = srcTables;
    }

    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

}
