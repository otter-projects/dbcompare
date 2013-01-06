package com.zavakid.dbcompare.table.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.zavakid.dbcompare.table.DiffDescription;
import com.zavakid.dbcompare.table.Record;
import com.zavakid.dbcompare.table.Table;

/**
 * No-Thread-safe
 * 
 * @author Zavakid 2012-12-12 下午7:04:56
 * @since 0.0.1
 */
public class MappedRecord implements Record {

    private Map<String, String> map = new HashMap<String, String>();
    private Table               table;

    public MappedRecord(Table table){
        this.table = table;
    }

    @Override
    public String getValue(String name) {
        return map.get(name);
    }

    @Override
    public String putValue(String name, String value) {
        return map.put(name, value);
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public DiffDescription diff(Record another) {
        return new DefaultDiffDexcription(this, another);
    }

    @Override
    public Set<String> getColumnNames() {
        return map.keySet();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Record)) {
            return false;
        }

        Record another = (Record) obj;

        if (this.getColumnNames().equals(another.getColumnNames())) {
            return true;
        }

        return false;

    }

    @Override
    public int hashCode() {
        return this.hashCode() + this.map.hashCode();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(table.getFullName());
        for (Entry<String, String> e : this.map.entrySet()) {
            sb.append(" [");
            sb.append(e.getKey()).append(" : ").append(e.getValue());
            sb.append("]");
        }

        return sb.toString();
    }
}
