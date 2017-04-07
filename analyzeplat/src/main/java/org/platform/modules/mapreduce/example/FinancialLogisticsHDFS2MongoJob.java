package org.platform.modules.mapreduce.example;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;
import org.bson.Document;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoJob;
import org.platform.modules.mapreduce.base.BaseHDFS2MongoV2Mapper;

public class FinancialLogisticsHDFS2MongoJob extends BaseHDFS2MongoJob {
	@Override
	public Class<? extends BaseHDFS2MongoV2Mapper> getMapperClass() {
		return FinancialLogisticsHDFS2MongoMapper.class;
	}

	
	
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new FinancialLogisticsHDFS2MongoJob(), args);  
	        System.exit(exitCode); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class FinancialLogisticsHDFS2MongoMapper extends BaseHDFS2MongoV2Mapper {

	@Override
	protected void buildDocument(Map<String, Object> record, List<Document> documents) {
		Document document1 = new Document();
		document1.put("province", record.get("province"));
		document1.put("city", record.get("city"));
		document1.put("county", record.get("county"));
		document1.put("address", record.get("address"));
		documents.add(document1);
		Document document2 = new Document();
		document2.put("province", record.get("linkProvince"));
		document2.put("city", record.get("linkCity"));
		document2.put("county", record.get("lingCounty"));
		document2.put("address", record.get("linkAddress"));
		documents.add(document2);
	}

	
	
}
