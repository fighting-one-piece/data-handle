package org.cisiondata.modules.jstorm.spout.kafka.trident;

import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.state.State;
import backtype.storm.task.OutputCollector;

@SuppressWarnings("rawtypes")
public class TridentKafkaState implements State {
	
    public static final Logger LOG = LoggerFactory.getLogger(TridentKafkaState.class);

    @SuppressWarnings("unused")
	private KafkaProducer producer;
    @SuppressWarnings("unused")
	private OutputCollector collector;

	private TridentTupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;

    public TridentKafkaState withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaState withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }
    
    @Override
	public void prepareCommit(long txid) {
    	Validate.notNull(mapper, "mapper can not be null");
        Validate.notNull(topicSelector, "topicSelector can not be null");
	}

	@Override
	public void commit(long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

	/**
    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is Noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is Noop.");
    }
    */

    public void prepare(Properties options) {
        Validate.notNull(mapper, "mapper can not be null");
        Validate.notNull(topicSelector, "topicSelector can not be null");
        producer = new KafkaProducer(options);
    }

    /**
    @SuppressWarnings("unchecked")
	public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        String topic = null;
        try {
            List<Future<RecordMetadata>> futures = new ArrayList<>(tuples.size());
            for (TridentTuple tuple : tuples) {
                topic = topicSelector.getTopic(tuple);

                if(topic != null) {
                    Future<RecordMetadata> result = producer.send(new ProducerRecord(topic,
                            mapper.getKeyFromTuple(tuple), mapper.getMessageFromTuple(tuple)));
                    futures.add(result);
                } else {
                    LOG.warn("skipping key = " + mapper.getKeyFromTuple(tuple) + ", topic selector returned null.");
                }
            }

            List<ExecutionException> exceptions = new ArrayList<>(futures.size());
            for (Future<RecordMetadata> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    exceptions.add(e);
                }
            }

            if(exceptions.size() > 0){
                String errorMsg = "Could not retrieve result for messages " + tuples + " from topic = " + topic 
                        + " because of the following exceptions: \n";
                for (ExecutionException exception : exceptions) {
                    errorMsg = errorMsg + exception.getMessage() + "\n";
                }
                LOG.error(errorMsg);
                throw new FailedException(errorMsg);
            }
        } catch (Exception ex) {
            String errorMsg = "Could not send messages " + tuples + " to topic = " + topic;
            LOG.warn(errorMsg, ex);
            throw new FailedException(errorMsg, ex);
        }
    }
    */

}
