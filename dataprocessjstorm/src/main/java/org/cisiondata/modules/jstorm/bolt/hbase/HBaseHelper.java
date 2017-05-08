package org.cisiondata.modules.jstorm.bolt.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.cisiondata.utils.bigdata.HBaseUtils;
import org.cisiondata.utils.json.GsonUtils;
import org.cisiondata.utils.serde.SerializerUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class HBaseHelper {
	
	private List<Tuple> tuples = null;
	
	private List<String> datas = null;
	
	private int batchSize = 1000;
	
	private OutputCollector collector = null;
	
	public HBaseHelper(int batchSize, OutputCollector collector) {
		this.tuples = new ArrayList<Tuple>();
		this.datas = new ArrayList<String>();
		if (batchSize > 0) this.batchSize = batchSize;
		this.collector = collector;
	}
	
	public void add(Tuple tuple) {
		System.out.println("hbase: " + tuple.getString(0));
		tuples.add(tuple);
		datas.add(tuple.getString(0));
		if (tuples.size() == batchSize) {
			bulkInsert(datas);
			datas.clear();
			ack();
		}
	}
	
	public void ack() {
		for (int i = 0, len = tuples.size(); i < len; i++) {
			collector.ack(tuples.get(i));
		}
		tuples.clear();
	}
	
	public void fail(Exception e) {
		collector.reportError(e);
		for (int i = 0, len = tuples.size(); i < len; i++) {
			collector.fail(tuples.get(i));
		}
		tuples.clear();
		datas.clear();
	}
	
	public void bulkInsert(List<String> datas) {
		if (null == datas || datas.size() == 0) return;
		Map<String, List<Put>> map = new HashMap<String, List<Put>>();
		Map<String, Object> source = null;
		for (int i = 0, len = datas.size(); i < len; i++) {
			source = GsonUtils.fromJsonToMap(datas.get(i));
			source.remove("index");
			String tableName = String.valueOf(source.remove("type"));
			List<Put> puts = map.get(tableName);
			if (null == puts) {
				puts = new ArrayList<Put>();
				map.put(tableName, puts);
			}
			String rowKey = String.valueOf(source.remove("_id"));
			Put put = new Put(Bytes.toBytes(rowKey));
			for (Map.Entry<String, Object> entry : source.entrySet()) {
				put.addColumn(Bytes.toBytes("i"), Bytes.toBytes(entry.getKey()), SerializerUtils.write(entry.getValue()));
			}
			puts.add(put);
		}
		
		for (Map.Entry<String, List<Put>> entry : map.entrySet()) {
			try {
				HBaseUtils.insertRecords(entry.getKey(), entry.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
