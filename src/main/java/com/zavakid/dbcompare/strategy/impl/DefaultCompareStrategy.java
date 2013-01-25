/*
   Copyright 2013 Zava (http://www.zavakid.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.zavakid.dbcompare.strategy.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zavakid.dbcompare.common.Utils;
import com.zavakid.dbcompare.config.Configuration;
import com.zavakid.dbcompare.strategy.CompareStrategy;
import com.zavakid.dbcompare.table.Table;
import com.zavakid.dbcompare.table.TablePair;
import com.zavakid.dbcompare.work.SmartCompareWorker;

/**
 * simple multithread compare strategy
 * 
 * @author Zavakid 2013-1-25 下午6:09:05
 * @since 0.0.1
 */
public class DefaultCompareStrategy implements CompareStrategy {

    private static final Logger log = LoggerFactory.getLogger(DefaultCompareStrategy.class);

    @Override
    public void compare(TablePair tablePair, ExecutorService workers, Configuration configuration) throws Exception {
        CompletionService completionService = new ExecutorCompletionService(workers);

        Table srcTable = tablePair.getSrcTable();
        Table destTable = tablePair.getDestTable();

        int srcCount = srcTable.getCount();
        log.info(Utils.SPLIT + "find {} from source table {}" + Utils.SPLIT, srcCount, srcTable.getFullName());

        int destCount = destTable.getCount();
        log.info(Utils.SPLIT + "find {} from dest table {}" + Utils.SPLIT, destCount, destTable.getFullName());

        int pages = srcCount / configuration.getMaxBatchSize();
        if (srcCount % configuration.getMaxBatchSize() != 0) {
            pages++;
        }
        int page = 0;
        for (; page < configuration.getMaxWorkers() && page < pages; page++) { // 开始提交任务
            int start = page * configuration.getMaxBatchSize();
            log.info(Utils.SPLIT + "[start : {} , end {}]" + Utils.SPLIT, start,
                     start + configuration.getMaxBatchSize());
            completionService.submit(buildSmartWorker(srcTable, destTable, start, configuration.getMaxBatchSize()));
        }

        for (; page < pages; page++) {
            completionService.take(); // 返回一个任务，继续下一个
            int start = page * configuration.getMaxBatchSize();
            log.info(Utils.SPLIT + "[start : {} , end {}]" + Utils.SPLIT, start,
                     start + configuration.getMaxBatchSize());
            completionService.submit(buildSmartWorker(srcTable, destTable, start, configuration.getMaxBatchSize()));
        }

        int workerNumber = pages < configuration.getMaxWorkers() ? pages : configuration.getMaxWorkers();
        for (int i = 0; i < workerNumber; i++) {
            completionService.take(); // 收集完所有的任务
        }

    }

    /**
     * @param srcTable
     * @param destTable
     * @param start
     * @param maxBatchSize
     * @return
     */
    private Callable buildSmartWorker(Table srcTable, Table destTable, int start, int maxBatchSize) {
        return new SmartCompareWorker(srcTable, destTable, start, maxBatchSize);
    }

}
