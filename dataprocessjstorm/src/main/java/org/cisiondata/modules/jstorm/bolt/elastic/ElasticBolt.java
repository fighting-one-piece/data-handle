package org.cisiondata.modules.jstorm.bolt.elastic;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ElasticBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private int batchSize = 1000;
	
	private ElasticHelper elasticHelper = null;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		elasticHelper = new ElasticHelper(batchSize, collector);
	}

	public void execute(Tuple input) {
		try {
			elasticHelper.add(input);
		} catch (Exception e) {
			elasticHelper.fail(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	

}
