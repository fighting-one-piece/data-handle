package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public abstract class BaseHDFS2MongoV2Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2MongoV2Mapper.class);
	
	private MongoClient mongoClient = null;
	
	private String databaseName = null;
	
	private String collectionName = null;
	
	private Gson gson = null;
	
	private int batchSize = 1000;
	
	private List<Document> records = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.databaseName = (String) context.getConfiguration().get("databaseName");
		this.collectionName = (String) context.getConfiguration().get("collectionName");
		this.batchSize = Integer.parseInt(String.valueOf(context.getConfiguration().get("batchSize", "1000")));
		this.gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.records = new ArrayList<Document>(this.batchSize);
		List<ServerAddress> serverAddressList = new ArrayList<ServerAddress>();  
        ServerAddress serverAddress01 = new ServerAddress("192.168.0.20", 27018);  
        ServerAddress serverAddress02 = new ServerAddress("192.168.0.115", 27018);  
        serverAddressList.add(serverAddress01);  
        serverAddressList.add(serverAddress02);  
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();  
        MongoCredential credential = MongoCredential.createCredential(  
                "root", "admin", "123456".toCharArray());  
        credentials.add(credential);  
        mongoClient = new MongoClient(serverAddressList, credentials);  
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Map<String, Object> record = gson.fromJson(value.toString(), Map.class);
			buildDocument(record, records);
			if (records.size() > this.batchSize) {
				bulkInsertDocuments(records);
				records.clear();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	protected abstract void buildDocument(Map<String, Object> record, List<Document> documents);
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
		if (!records.isEmpty()) {
			bulkInsertDocuments(records);
			records.clear();
		}
		mongoClient.close();
	}
	
	/**
	 * 批量插入Mongo
	 * @param documents
	 */
	private void bulkInsertDocuments(List<Document> documents) {
		if (null == documents || documents.isEmpty()) return;
		MongoDatabase database = mongoClient.getDatabase(this.databaseName);
        MongoCollection<Document> collection = database.getCollection(this.collectionName);
        collection.insertMany(documents);
	}

}

