package com.zavakid.dbcompare.table.impl;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * @author Zavakid 2013-1-7 上午3:37:29
 * @since 0.0.1
 */
public class DdlUtilsCache {

    private static final Map<InnerKey, Database> DATABASE_CACHE = new MapMaker().makeComputingMap(new Function<InnerKey, Database>() {

                                                                    @Override
                                                                    public Database apply(InnerKey key) {
                                                                        return findDatasource(key);
                                                                    }

                                                                });

    public static final Database getDatabase(DataSource datasource, String schema) {
        return getDatabase(datasource, schema, true);
    }

    public static final Database getDatabase(DataSource datasource, String schema, boolean cacheFirst) {
        Database result;
        InnerKey key = new InnerKey(datasource, schema);
        if (cacheFirst) {
            result = DATABASE_CACHE.get(key);
        } else {
            result = findDatasource(key);
            // update cache, the DATABASE_CACHE is thread safe
            DATABASE_CACHE.put(key, result);
        }
        return result;
    }

    private static Database findDatasource(InnerKey key) {
        Platform platform = PlatformFactory.createNewPlatformInstance(key.datasource);
        return platform.readModelFromDatabase(key.schema);
    }

    /**
     * just for object instance
     */
    private static final class InnerKey {

        private DataSource datasource;
        private String     schema;

        public InnerKey(DataSource datasource, String schema){
            this.datasource = datasource;
            this.schema = schema;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((datasource == null) ? 0 : datasource.hashCode());
            result = prime * result + ((schema == null) ? 0 : schema.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            InnerKey other = (InnerKey) obj;
            if (datasource == null) {
                if (other.datasource != null) return false;
            } else if (!datasource.equals(other.datasource)) return false;
            if (schema == null) {
                if (other.schema != null) return false;
            } else if (!schema.equals(other.schema)) return false;
            return true;
        }

    }

}
