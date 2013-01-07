package com.zavakid.dbcompare.work;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zavakid.dbcompare.common.Utils;
import com.zavakid.dbcompare.table.DiffDescription;
import com.zavakid.dbcompare.table.Record;
import com.zavakid.dbcompare.table.Table;

/**
 * @author Zavakid 2013-1-7 上午3:41:02
 * @since 0.0.1
 */
public class SmartCompareWorker implements Callable<Exception> {

    private static final Logger log        = LoggerFactory.getLogger(SmartCompareWorker.class);

    private static String       START_LOG  = Utils.SPLIT + "start %s from %s to %s" + Utils.SPLIT;
    private static String       OK_LOG     = Utils.SPLIT + "OK: %s from %s to %s" + Utils.SPLIT;

    private static final String SRC_WHERE  = " order by %s asc limit %s,%s";
    private static final String DEST_WHERE = " where %s in ( %s )";

    private Table               srcTable;
    private Table               destTable;
    private int                 start;
    private int                 size;

    public SmartCompareWorker(Table srcTable, Table destTable, int start, int size){
        this.srcTable = srcTable;
        this.destTable = destTable;
        this.start = start;
        this.size = size;
    }

    @Override
    public Exception call() throws Exception {
        log.info(String.format(START_LOG, srcTable.getFullName(), start, start + size));

        boolean right = true;
        try {
            String where = String.format(SRC_WHERE, srcTable.getMainPkName(), start, size);
            Map<String, Record> srcRecords = srcTable.findRecord(where);

            Set<String> pks = srcRecords.keySet();
            String destWhere = String.format(DEST_WHERE, srcTable.getPkNames().get(0), StringUtils.join(pks, ","));
            Map<String, Record> destRecords = destTable.findRecord(destWhere);

            for (Entry<String, Record> e : srcRecords.entrySet()) {
                String pk = e.getKey();
                Record srcRecord = e.getValue();
                Record destRecord = destRecords.remove(pk);

                DiffDescription diffDescription = srcRecord.diff(destRecord);
                diffDescription.diff();
                if (diffDescription.isDifferent()) {
                    log.error(diffDescription.showResult());
                    right = false;
                }
            }

            if (right) {
                log.info(String.format(OK_LOG, srcTable.getFullName(), start, start + size));
            }
        } catch (Exception e) {
            log.error("error happens", e);
            return e;
        }

        return null;
    }
}
