package org.cisiondata.modules.storm.bolt.hbase;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

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
