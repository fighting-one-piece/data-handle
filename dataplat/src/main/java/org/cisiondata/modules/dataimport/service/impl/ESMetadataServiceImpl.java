package org.cisiondata.modules.dataimport.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.client.ESClient;
import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.dataimport.service.IESMetadataService;
import org.cisiondata.utils.exception.BusinessException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

@Service("esMetadataService")
public class ESMetadataServiceImpl implements IESMetadataService,InitializingBean{

	Logger LOG = LoggerFactory.getLogger(ESMetadataServiceImpl.class);

	//index集合
	private Set<String> indices = null;
	//type集合
	private Set<String> types = null;
	//index，type映射
	private Map<String, List<String>> index_types_mapping = null;
	//type,attribute映射
	Map<String, Map<String, List<String>>> index_type_attributes_mapping = null;
	
	@Override
	public void afterPropertiesSet() throws Exception {
		initMetadataCache();
	}
	
	@Override
	public Set<String> readIndices() throws BusinessException {
		if (null == indices) initMetadataCache();
		return indices;
	}
	
	@Override
	public List<String> readIndexTypes(String index) throws BusinessException {
		if (StringUtils.isBlank(index)) throw new BusinessException(ResultCode.PARAM_NULL);
		if (null == index_types_mapping) initMetadataCache();
		if (!index_types_mapping.containsKey(index)) return new ArrayList<String>();
		return index_types_mapping.get(index);
	}
	
	@Override
	public List<String> readIndexTypeAttributes(String index, String type) throws BusinessException {
		if (StringUtils.isBlank(index) || StringUtils.isBlank(type)) throw new BusinessException(ResultCode.PARAM_NULL);
		if (null == index_type_attributes_mapping) initMetadataCache();
		Map<String,List<String>> type_attributes_mapping = index_type_attributes_mapping.get(index);
		if (null == type_attributes_mapping) return new ArrayList<String>();
		List<String> attributes = type_attributes_mapping.get(type);
		if (null == attributes) return new ArrayList<String>();
		return attributes;
	}
	
	private void initMetadataCache(){
		indices = new HashSet<String>();
		types = new HashSet<String>();
		index_types_mapping = new HashMap<String, List<String>>();
		index_type_attributes_mapping = new HashMap<String, Map<String, List<String>>>();
		try {
			IndicesAdminClient indicesAdminClient = ESClient.getInstance().getClient125().admin().indices();
			GetMappingsResponse getMappingsResponse = indicesAdminClient.getMappings(new GetMappingsRequest()).get();
			ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getMappingsResponse
					.getMappings();
			Iterator<ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>>> mappingIterator = mappings
					.iterator();
			while (mappingIterator.hasNext()) {
				ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> objectObjectCursor = mappingIterator
						.next();
				String index = objectObjectCursor.key;
				if (index.startsWith(".marvel-es"))
					continue;
				indices.add(index);
				List<String> indexTypes = index_types_mapping.get(index);
				if (null == indexTypes) {
					indexTypes = new ArrayList<String>();
					index_types_mapping.put(index, indexTypes);
				}
				ImmutableOpenMap<String, MappingMetaData> immutableOpenMap = objectObjectCursor.value;
				ObjectLookupContainer<String> keys = immutableOpenMap.keys();
				Iterator<ObjectCursor<String>> keysIterator = keys.iterator();
				while (keysIterator.hasNext()) {
					String type = keysIterator.next().value;
					types.add(type);
					indexTypes.add(type);
					Map<String, List<String>> index_type_attributes = index_type_attributes_mapping.get(index);
					if (null == index_type_attributes_mapping.get(index)) {
						index_type_attributes = new HashMap<String, List<String>>();
						index_type_attributes_mapping.put(index, index_type_attributes);
					}
					MappingMetaData mappingMetaData = immutableOpenMap.get(type);
					Map<String, Object> mapping = mappingMetaData.getSourceAsMap();
					if (mapping.containsKey("properties")) {
						@SuppressWarnings("unchecked")
						Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
						List<String> type_attributes = new ArrayList<String>();
						for (String attribute : properties.keySet()) {
							type_attributes.add(attribute);
						}
						index_type_attributes.put(type, type_attributes);
						index_type_attributes_mapping.put(index, index_type_attributes);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}
	}
	
	
}
