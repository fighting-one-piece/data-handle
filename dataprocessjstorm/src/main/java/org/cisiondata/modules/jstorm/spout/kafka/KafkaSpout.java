package org.cisiondata.modules.jstorm.spout.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cisiondata.modules.jstorm.spout.kafka.PartitionManager.KafkaMessageId;
import org.cisiondata.modules.jstorm.spout.kafka.trident.GlobalPartitionInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
@SuppressWarnings("serial")
public class KafkaSpout extends BaseRichSpout {
    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    ZkState _state;

    long _lastUpdateMs = 0;

    int _currPartitionIndex = 0;

    public KafkaSpout(SpoutConfig spoutConf) {
        _spoutConfig = spoutConf;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;
        String topologyInstanceId = context.getTopologyId();
        Map<String, Object> stateConf = new HashMap<>(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        _state = new ZkState(stateConf);

        _connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

        // using TransactionalState like this is a hack
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (_spoutConfig.hosts instanceof StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, conf,
                    _spoutConfig, _state, context.getThisTaskIndex(),
                    totalTasks, topologyInstanceId);
        } else {
            _coordinator = new ZkCoordinator(_connections, conf,
                    _spoutConfig, _state, context.getThisTaskIndex(),
                    totalTasks, topologyInstanceId);
        }

        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setOffsetData(pm.getPartition(), pm.getOffsetData());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {

            try {
                // in case the number of managers decreased
                _currPartitionIndex = _currPartitionIndex % managers.size();
                EmitState state = managers.get(_currPartitionIndex).next(_collector);
                if (state != EmitState.EMITTED_MORE_LEFT) {
                    _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
                }
                if (state != EmitState.NO_EMITTED) {
                    break;
                }
            } catch (FailedFetchException e) {
                LOG.warn("Fetch failed", e);
                _coordinator.refresh();
            }
        }

        long diffWithNow = System.currentTimeMillis() - _lastUpdateMs;

        /*
             As far as the System.currentTimeMillis() is dependent on System clock,
             additional check on negative value of diffWithNow in case of external changes.
         */
        if (diffWithNow > _spoutConfig.stateUpdateIntervalMs || diffWithNow < 0) {
            commit();
        }
    }

    private PartitionManager getManagerForPartition(int partition) {
        for (PartitionManager partitionManager: _coordinator.getMyManagedPartitions()) {
            if (partitionManager.getPartition().partition == partition) {
                return partitionManager;
            }
        }
        return null;
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.ack(id.offset);
        } else {
            // managers for partitions changed - try to find new manager responsible for that partition
            PartitionManager newManager = getManagerForPartition(id.partition.partition);
            if (newManager != null) {
                newManager.ack(id.offset);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.fail(id.offset);
        } else {
            // managers for partitions changed - try to find new manager responsible for that partition
            PartitionManager newManager = getManagerForPartition(id.partition.partition);
            if (newManager != null) {
                newManager.fail(id.offset);
            }
        }
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       if (!Strings.isNullOrEmpty(_spoutConfig.outputStreamId)) {
            declarer.declareStream(_spoutConfig.outputStreamId, _spoutConfig.scheme.getOutputFields());
        } else {
            declarer.declare(_spoutConfig.scheme.getOutputFields());
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
        Map<String, Object> configuration = super.getComponentConfiguration();
        if (configuration == null) {
            configuration = new HashMap<>();
        }
        String configKeyPrefix = "config.";
        configuration.put(configKeyPrefix + "topics", this._spoutConfig.topic);
        StringBuilder zkServers = new StringBuilder();
        if (_spoutConfig.zkServers != null && _spoutConfig.zkServers.size() > 0) {
            for (String zkServer : this._spoutConfig.zkServers) {
                zkServers.append(zkServer + ":" + this._spoutConfig.zkPort + ",");
            }
            configuration.put(configKeyPrefix + "zkServers", zkServers.toString());
        }
        BrokerHosts brokerHosts = this._spoutConfig.hosts;
        String zkRoot = this._spoutConfig.zkRoot + "/" + this._spoutConfig.id;
        if (brokerHosts instanceof ZkHosts) {
            ZkHosts zkHosts = (ZkHosts) brokerHosts;
            configuration.put(configKeyPrefix + "zkNodeBrokers", zkHosts.brokerZkPath);
        } else if (brokerHosts instanceof StaticHosts) {
            StaticHosts staticHosts = (StaticHosts) brokerHosts;
            GlobalPartitionInformation globalPartitionInformation = staticHosts.getPartitionInformation();
            boolean useTopicNameForPath = globalPartitionInformation.getbUseTopicNameForPartitionPathId();
            if (useTopicNameForPath) {
                zkRoot += ("/" + this._spoutConfig.topic);
            }
            List<Partition> partitions = globalPartitionInformation.getOrderedPartitions();
            StringBuilder staticPartitions = new StringBuilder();
            StringBuilder leaderHosts = new StringBuilder();
            for (Partition partition: partitions) {
                staticPartitions.append(partition.partition + ",");
                leaderHosts.append(partition.host.host + ":" + partition.host.port).append(",");
            }
            configuration.put(configKeyPrefix + "partitions", staticPartitions.toString());
            configuration.put(configKeyPrefix + "leaders", leaderHosts.toString());
        }
        configuration.put(configKeyPrefix + "zkRoot", zkRoot);
        return configuration;
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

}
