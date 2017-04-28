package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BaseES2HDFSMapper extends Mapper<Text, LinkedMapWritable, NullWritable, Text> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseES2HDFSMapper.class);
	
	private long record_split_num = 10000;
	
	private long record_num = 0;
	
	private int file_num = 0;
	
	private Gson gson = null;
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		String num = context.getConfiguration().get("record.split.num", "10000");
		record_split_num = Long.parseLong(num);
		gson = new GsonBuilder().serializeSpecialFloatingPointValues()
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text key, LinkedMapWritable value, Context context)
			throws IOException, InterruptedException {
		if (record_num % record_split_num == 0) file_num++;
		Map<String, Object> result = new HashMap<String, Object>();
		for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
			if ("_metadata".equalsIgnoreCase(entry.getKey().toString())) {
				Map<String, Object> map = gson.fromJson(entry.getValue().toString(), Map.class);
				if (map.containsKey("_id")) {
					result.put("_id", map.get("_id"));
				}
			} else {
				result.put(entry.getKey().toString(), entry.getValue().toString());
			}
		}
		multipleOutputs.write(NullWritable.get(), new Text(gson.toJson(result)), "records-" + file_num);
		record_num++;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		LOG.info("total record num : " + record_num + " file num : " + file_num);
	}

}
