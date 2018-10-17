package com.alibaba.datax.plugin.writer.eswriter;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.gson.Gson;

public class ESWriter extends Writer {

	public static class Job extends Writer.Job {
		
		private Configuration originalConfiguration = null;
		
		@Override
		public void preCheck() {
			super.preCheck();
		}

		@Override
		public void preHandler(Configuration jobConfiguration) {
			super.preHandler(jobConfiguration);
		}
		
		@Override
		public void init() {
			this.originalConfiguration = super.getPluginJobConf();
		}

		@Override
		public void prepare() {
			super.prepare();
		}
		
		@Override
		public void post() {
			super.post();
		}
		
		@Override
		public void postHandler(Configuration jobConfiguration) {
			super.postHandler(jobConfiguration);
		}

		@Override
		public void destroy() {
			
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			List<Configuration> writerSplitConfiguration = new ArrayList<Configuration>();
			for (int i = 0; i < mandatoryNumber; i++) {
				writerSplitConfiguration.add(this.originalConfiguration);
			}
			return writerSplitConfiguration;
		}
		
	}
	
	public static class Task extends Writer.Task {
		
		private Configuration writerSliceConfiguration = null;
		
		private String esClusterName = null;
		
		private String esClusterIP = null;

		private Integer esClusterPort = null;
		
		private String esIndex = null;
		
		private String esType = null;
		
		private String attributeNameString = null;
		
		private String attributeNameSplit = null;
		
		private String[] attributeNames = null;
		
		private String className = null;
		
		private Gson gson = null;
		
		private TransportClient client = null;
		
		private Integer batchSize = null;
		
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		@Override
		public void preCheck() {
			super.preCheck();
		}
		
		@Override
		public void preHandler(Configuration jobConfiguration) {
			super.preHandler(jobConfiguration);
		}
		
		@Override
		public void init() {
			this.writerSliceConfiguration = super.getPluginJobConf();
			this.esClusterName = writerSliceConfiguration.getString(Key.esClusterName);
			this.esClusterIP = writerSliceConfiguration.getString(Key.esClusterIP);
			this.esClusterPort = writerSliceConfiguration.getInt(Key.esClusterPort, 9300);
			this.esIndex = writerSliceConfiguration.getString(Key.esIndex);
			this.esType = writerSliceConfiguration.getString(Key.esType);
			this.attributeNameString = writerSliceConfiguration.getString(Key.attributeNameString);
			this.attributeNameSplit = writerSliceConfiguration.getString(Key.attributeNameSplit, ",");
			attributeNames = attributeNameString.split(attributeNameSplit);
			this.className = writerSliceConfiguration.getString(Key.className);
			this.batchSize = writerSliceConfiguration.getInt(Key.batchSize, 1000);
			this.gson = new Gson();
		}
		
		@Override
		public void prepare() {
			super.prepare();
			Settings settings = Settings.builder().put("cluster.name", esClusterName)
					.put("client.tansport.sniff", true).build();
			client = new PreBuiltXPackTransportClient(settings);
			List<EsServerAddress> serverAddress = new ArrayList<EsServerAddress>();
			String[] esClusterIPs = esClusterIP.contains(",") ? 
					esClusterIP.split(",") : new String[]{esClusterIP};
			for (int i = 0, len = esClusterIPs.length; i < len; i++) {
				serverAddress.add(new EsServerAddress(esClusterIPs[i], esClusterPort));
			}
			for (EsServerAddress address : serverAddress) {
				client.addTransportAddress(new InetSocketTransportAddress(
						new InetSocketAddress(address.getHost(), address.getPort())));
			}
		}
		
		@Override
		public void post() {
			super.post();
		}
		
		@Override
		public void postHandler(Configuration jobConfiguration) {
			super.postHandler(jobConfiguration);
		}

		@Override
		public void destroy() {
			client.close();
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
			Record record = null;
			while ((record = lineReceiver.getFromReader()) != null) {
				writerBuffer.add(record);
				if (writerBuffer.size() >= this.batchSize) {
					bulkSaveOrUpdateES(writerBuffer);
					writerBuffer.clear();
				}
			}
			if (!writerBuffer.isEmpty()) {
				bulkSaveOrUpdateES(writerBuffer);
				writerBuffer.clear();
			}
		}
		
		private void bulkSaveOrUpdateES(List<Record> writerBuffer) {
			Record record = null;
			Object object = null;
			Map<String, String> attributeValueMap = null;
			List<ESEntity> entities = new ArrayList<ESEntity>();
			try {
				for (int w = 0, wlen = writerBuffer.size(); w < wlen; w++) {
					record = writerBuffer.get(w);
					object = Class.forName(className).newInstance();
					int fieldNum = record.getColumnNumber();
					if (null != record && fieldNum > 0) {
						attributeValueMap = new HashMap<String, String>();
						for (int i = 0; i < fieldNum; i++) {
							attributeValueMap.put(attributeNames[i].toLowerCase(), record.getColumn(i).asString());
						}
						for (Class<?> superClass = object.getClass(); 
								superClass != Object.class; superClass = superClass.getSuperclass()) {
				        	Field[] fields = superClass.getDeclaredFields();
				    		for (int i = 0, len = fields.length; i < len; i++) {
								Field field = fields[i];
								String fieldNameLowerCase = field.getName().toLowerCase();
								if (!attributeValueMap.containsKey(fieldNameLowerCase)) continue;
								String valueString = attributeValueMap.get(fieldNameLowerCase);
								Object value = convertValueByFieldType(field.getType(), valueString);
								if (field.isAccessible()) {
						            field.set(object, value);
						        } else {
						            field.setAccessible(true);
						            field.set(object, value);
						            field.setAccessible(false);
						        }
				    		}
				        }
						entities.add((ESEntity) object);
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
			bulkSaveOrUpdate(entities, esIndex, esType);
		}
		
		@SuppressWarnings("deprecation")
		private void bulkSaveOrUpdate(List<ESEntity> entities, String database, String table) {
			if (null == entities || entities.isEmpty()) return;
			BulkRequestBuilder prepareBulk = client.prepareBulk();
			for (ESEntity entity : entities) {
				IndexRequestBuilder irb = client.prepareIndex()
						.setIndex(database).setType(table).setId(entity.get_id());
				entity.remove_id();
				String source = gson.toJson(entity);
				irb.setSource(source);
				prepareBulk.add(irb);
			}
			BulkResponse bulkResponse = prepareBulk.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				LOG.info(bulkResponse.buildFailureMessage());
			}
		}
		
		private Object convertValueByFieldType(Class<?> type, Object value) {
			String valueString = null;
			if (null != value) {
				valueString = String.valueOf(value);
				if (valueString.contains("\"")) {
					valueString = valueString.replaceAll("\"", "");
				} 
				if (valueString.contains("“")) {
					valueString = valueString.replaceAll("“", "");
				}
				if (valueString.contains("”")) {
					valueString = valueString.replaceAll("”", "");
				}
			}
	    	Object finalValue = valueString;
	    	if (String.class.isAssignableFrom(type)) {
	    		finalValue = StringUtils.isBlank(valueString) ? "NA" : valueString;
			} else if (Boolean.class.isAssignableFrom(type)) {
	    		finalValue = StringUtils.isBlank(valueString) ? 
	    				Boolean.FALSE : Boolean.parseBoolean(valueString);
			} else if (Integer.class.isAssignableFrom(type)) {
				try {
					finalValue = StringUtils.isBlank(valueString) ? 0 : Integer.parseInt(valueString);
				} catch (Exception e) {
					finalValue = 0;
				}
			} else if (Long.class.isAssignableFrom(type)) {
				try {
					finalValue = StringUtils.isBlank(valueString) ? 0 : Long.parseLong(valueString);
				} catch (Exception e) {
					finalValue = 0l;
				}
			} else if (Float.class.isAssignableFrom(type)) {
				try {
					if (StringUtils.isBlank(valueString)) {
						finalValue = 0f;
					} else {
						valueString = valueString.replace("￥", "");
						valueString = valueString.replace("$", "");
						valueString = valueString.replace("、", "");
						finalValue = Float.parseFloat(valueString);
					}
				} catch (Exception e) {
					finalValue = 0f;
				}
			} else if (Double.class.isAssignableFrom(type)) {
				try {
					if (StringUtils.isBlank(valueString)) {
						finalValue = 0d;
					} else {
						valueString = valueString.replace("￥", "");
						valueString = valueString.replace("$", "");
						valueString = valueString.replace("、", "");
						finalValue = Double.parseDouble(valueString);
					}
				} catch (Exception e) {
					finalValue = 0d;
				}
			} else if (Date.class.isAssignableFrom(type)) {
				try {
					valueString = StringUtils.isBlank(valueString) ? 
							DateFormatter.TIME.get().format(new Date()) : valueString;
					finalValue = DateFormatter.TIME.get().parse(valueString);
				} catch (ParseException e) {
					LOG.error(e.getMessage(), e);
				}
			} 
	    	return finalValue;
	    }
		
	}
	
}
