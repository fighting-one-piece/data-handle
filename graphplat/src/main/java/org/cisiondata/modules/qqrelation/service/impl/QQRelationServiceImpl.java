package org.cisiondata.modules.qqrelation.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.cisiondata.modules.qqrelation.service.IQQRelationService;
import org.cisiondata.utils.exception.BusinessException;
import org.cisiondata.utils.json.GsonUtils;
import org.cisiondata.utils.titan.TitanUtils;
import org.springframework.stereotype.Service;

import com.thinkaurelius.titan.core.TitanGraph;

@Service("qqRelationService")
public class QQRelationServiceImpl implements IQQRelationService {
	
	@Override
	public void insertQQNodes(List<String> nodes) throws BusinessException {
		TitanGraph graph = TitanUtils.getInstance().getGraph();
		for (int i = 0, len = nodes.size(); i < len; i++) {
			Vertex vertex = graph.addVertex("qq");
			Map<String, Object> node = GsonUtils.fromJsonToMap(nodes.get(i));
			for (Map.Entry<String, Object> entry : node.entrySet()) {
				if ("_id".equals(entry.getKey())) continue;
				vertex.property(entry.getKey(), entry.getValue());
			}
		}
	}
	
	@Override
	public void insertQQQunNodes(List<String> nodes) throws BusinessException {
		TitanGraph graph = TitanUtils.getInstance().getGraph();
		for (int i = 0, len = nodes.size(); i < len; i++) {
			Vertex vertex = graph.addVertex("qqqun");
			Map<String, Object> node = GsonUtils.fromJsonToMap(nodes.get(i));
			for (Map.Entry<String, Object> entry : node.entrySet()) {
				if ("_id".equals(entry.getKey())) continue;
				vertex.property(entry.getKey(), entry.getValue());
			}
		}
	}

}
