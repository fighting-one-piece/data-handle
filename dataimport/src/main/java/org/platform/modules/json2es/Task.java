package org.platform.modules.json2es;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.platform.modules.elastic.ElasticClient;
import org.platform.utils.json.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task implements Runnable {
	
	private Logger LOG = LoggerFactory.getLogger(Task.class);
	
	private String filePath = null;
	
	private String esIndex = null;
	
	private String esType = null;
	
	private Integer batchSize = 100;
	
	private Semaphore semaphore = null;
	
	public Task(String filePath, String esIndex, String esType, Semaphore semaphore) {
		this(filePath, esIndex, esType, 100, semaphore);
	}
	
	public Task(String filePath, String esIndex, String esType, Integer batchSize, Semaphore semaphore) {
		super();
		this.filePath = filePath;
		this.esIndex = esIndex;
		this.esType = esType;
		this.batchSize = batchSize;
		this.semaphore = semaphore;
		LOG.info("{} {} {} {}", filePath, esIndex, esType, batchSize);
	}

	@Override
	public void run() {
		System.out.println(filePath + " task start !");
		InputStream in = null;
		BufferedReader br = null;
		int recordNumber = 0;
		try {
			in = new FileInputStream(new File(filePath));
			br = new BufferedReader(new InputStreamReader(in), 5*1024*1024);
			List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
			String line = null;
			while (null != (line = br.readLine())) {
				try {
					Map<String, Object> record = GsonUtils.fromJsonToMap(line);
					handle(record);
					records.add(record);
					recordNumber++;
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (records.size() > batchSize) {
					bulkInsertES(records);
					records.clear();
				}
			}
			if (!records.isEmpty()) bulkInsertES(records);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			e.printStackTrace();
		} finally {
			try {
				if (null != in) in.close();
				if (null != br) br.close();
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
		semaphore.release();
		System.out.println(filePath + " release permit !");
		System.out.println(filePath + " task end ! handle record number " + recordNumber);
	}
	
	private void handle(Map<String, Object> record) {
		
	}
	
	private void bulkInsertES(List<Map<String, Object>> records) {
		if (null == records || records.isEmpty()) return;
		TransportClient client = (TransportClient) ElasticClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = records.size(); i < len; i++) {
			source = records.get(i);
			if (source.containsKey("_id")) {
				String _id = (String) source.remove("_id");
				irb = client.prepareIndex(this.esIndex, this.esType, _id);
				irb.setSource(source);
			} else {
				irb = client.prepareIndex(this.esIndex, this.esType).setSource(source);
			}
			bulkRequestBuilder.add(irb);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			System.out.println(bulkResponse.buildFailureMessage());
		}
	}
	
}
