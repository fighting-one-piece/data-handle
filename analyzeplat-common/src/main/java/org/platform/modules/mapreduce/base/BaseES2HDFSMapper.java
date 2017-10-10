package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.platform.utils.json.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseES2HDFSMapper extends Mapper<Text, LinkedMapWritable, NullWritable, Text> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseES2HDFSMapper.class);
	
	private long record_split_num = 10000;
	
	private long record_num = 0;
	
	private int file_num = 0;
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		String num = context.getConfiguration().get("record.split.num", "10000");
		record_split_num = Long.parseLong(num);
	}

	@Override
	protected void map(Text key, LinkedMapWritable value, Context context)
			throws IOException, InterruptedException {
		if (record_num % record_split_num == 0) file_num++;
		Map<String, Object> result = new HashMap<String, Object>();
		for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
			if ("_metadata".equalsIgnoreCase(entry.getKey().toString())) {
				Map<String, Object> map = GsonUtils.fromJsonToMap(entry.getValue().toString());
				if (map.containsKey("_id")) {
					result.put("_id", map.get("_id"));
				}
			} else {
				Object rvalue = entry.getValue();
				if (null == rvalue) continue;
				String rvalue_string = String.valueOf(rvalue);
				if (StringUtils.isBlank(rvalue_string) || 
						"NA".equals(rvalue_string) || "(null)".equals(rvalue_string)) continue;
				result.put(entry.getKey().toString(), rvalue_string);
			}
		}
		multipleOutputs.write(NullWritable.get(), new Text(GsonUtils.fromMapToJson(result)), "records-" + file_num);
		record_num++;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if(null != multipleOutputs) {
            multipleOutputs.close();
            multipleOutputs = null;
        }
		LOG.info("total record num : " + record_num + " file num : " + file_num);
	}

}
