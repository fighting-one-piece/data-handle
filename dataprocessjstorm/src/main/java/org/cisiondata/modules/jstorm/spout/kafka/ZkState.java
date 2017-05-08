package org.cisiondata.modules.jstorm.spout.kafka;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.cisiondata.modules.jstorm.spout.kafka.utils.ObjectReader;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

public class ZkState {
    private static final Logger LOG = LoggerFactory.getLogger(ZkState.class);
    CuratorFramework _curator;

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private CuratorFramework newCurator(Map stateConf) throws Exception {
        Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
        String serverPorts = "";
        for (String server : (List<String>) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)) {
            serverPorts = serverPorts + server + ":" + port + ",";
        }
        return CuratorFrameworkFactory.newClient(serverPorts,
                ObjectReader.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                ObjectReader.getInt(stateConf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
                new RetryNTimes(ObjectReader.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                        ObjectReader.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
    }

    public CuratorFramework getCurator() {
        assert _curator != null;
        return _curator;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public ZkState(Map stateConf) {
        stateConf = new HashMap(stateConf);

        try {
            _curator = newCurator(stateConf);
            _curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeJSON(String path, Map<Object, Object> data) {
        LOG.debug("Writing {} the data {}", path, data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }

    public void writeBytes(String path, byte[] bytes) {
        try {
            if (_curator.checkExists().forPath(path) == null) {
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, bytes);
            } else {
                _curator.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
	public Map<Object, Object> readJSON(String path) {
        try {
            byte[] b = readBytes(path);
            if (b == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parseWithException(new String(b, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] readBytes(String path) {
        try {
            if (_curator.checkExists().forPath(path) != null) {
                return _curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _curator.close();
        _curator = null;
    }
}
