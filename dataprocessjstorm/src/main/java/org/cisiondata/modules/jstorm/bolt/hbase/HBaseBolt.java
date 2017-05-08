package org.cisiondata.modules.jstorm.bolt.hbase;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HBaseBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private int batchSize = 2;
	
	private HBaseHelper hbaseHelper = null;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		hbaseHelper = new HBaseHelper(batchSize, collector);
	}

	public void execute(Tuple input) {
		try {
			hbaseHelper.add(input);
		} catch (Exception e) {
			hbaseHelper.fail(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	

}
