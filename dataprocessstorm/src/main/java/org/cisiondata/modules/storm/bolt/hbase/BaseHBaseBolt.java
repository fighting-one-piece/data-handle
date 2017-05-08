package org.cisiondata.modules.storm.bolt.hbase;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.bolt.AbstractHBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHBaseBolt extends AbstractHBaseBolt {

	private static final long serialVersionUID = 1L;
	
	private Logger LOG = LoggerFactory.getLogger(BaseHBaseBolt.class);
	
	private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private boolean writeToWAL = true;
    private int batchSize = 1000;
    private List<Mutation> batchMutations = null;
    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    private BatchHelper batchHelper = null;
	
    public BaseHBaseBolt(String tableName, HBaseMapper mapper) {
        super(tableName, mapper);
        this.batchMutations = new LinkedList<>();
    }

    public BaseHBaseBolt writeToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
        return this;
    }

    public BaseHBaseBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public BaseHBaseBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public BaseHBaseBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }
    
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
		super.prepare(stormConf, context, collector);
        this.batchHelper = new BatchHelper(batchSize, collector);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		System.out.println("hbase: " +input);
		System.out.println("hbase: " + input.getValueByField("_id"));
		try {
            if (batchHelper.shouldHandle(input)) {
                byte[] rowKey = this.mapper.rowKey(input);
                ColumnList cols = this.mapper.columns(input);
                List<Mutation> mutations = hBaseClient.constructMutationReq(rowKey, cols, writeToWAL? Durability.SYNC_WAL : Durability.SKIP_WAL);
                batchMutations.addAll(mutations);
                batchHelper.addBatch(input);
            }
            if (batchHelper.shouldFlush()) {
                this.hBaseClient.batchMutate(batchMutations);
                batchHelper.ack();
                batchMutations.clear();
            }
        } catch(Exception e){
        	LOG.error(e.getMessage(), e);
            batchHelper.fail(e);
            batchMutations.clear();
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
