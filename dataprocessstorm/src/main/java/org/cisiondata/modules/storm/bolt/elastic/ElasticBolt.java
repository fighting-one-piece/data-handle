package org.cisiondata.modules.storm.bolt.elastic;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ElasticBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private int batchSize = 2;
	
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
