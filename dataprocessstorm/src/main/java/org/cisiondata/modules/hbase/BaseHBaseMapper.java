package org.cisiondata.modules.hbase;

import static org.apache.storm.hbase.common.Utils.toBytes;
import static org.apache.storm.hbase.common.Utils.toLong;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class BaseHBaseMapper implements HBaseMapper {
	
	private static final long serialVersionUID = 1L;

    private String rowKeyField = null;
    private byte[] columnFamily = null;
    private Fields columnFields = null;
    private Fields counterFields = null;

    public BaseHBaseMapper(){
    }

    public BaseHBaseMapper withRowKeyField(String rowKeyField){
        this.rowKeyField = rowKeyField;
        return this;
    }

    public BaseHBaseMapper withColumnFields(Fields columnFields){
        this.columnFields = columnFields;
        return this;
    }

    public BaseHBaseMapper withCounterFields(Fields counterFields){
        this.counterFields = counterFields;
        return this;
    }

    public BaseHBaseMapper withColumnFamily(String columnFamily){
        this.columnFamily = columnFamily.getBytes();
        return this;
    }

    @Override
    public byte[] rowKey(Tuple tuple) {
        Object object = tuple.getValueByField(this.rowKeyField);
        return toBytes(object);
    }

    @Override
    public ColumnList columns(Tuple tuple) {
        ColumnList columnList = new ColumnList();
        if(this.columnFields != null){
            for(String field : this.columnFields){
            	if (!tuple.contains(field)) continue;
                columnList.addColumn(this.columnFamily, field.getBytes(), toBytes(tuple.getValueByField(field)));
            }
        }
        if(this.counterFields != null){
            for(String field : this.counterFields){
            	if (!tuple.contains(field)) continue;
                columnList.addCounter(this.columnFamily, field.getBytes(), toLong(tuple.getValueByField(field)));
            }
        }
        return columnList;
    }

}
