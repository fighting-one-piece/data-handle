package org.platform.modules.mapreduce.base;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.platform.utils.DateFormatter;
import org.platform.utils.IDGenerator;
import org.platform.utils.ValidateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class BaseHDFS2HDFSMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	protected Logger LOG = LoggerFactory.getLogger(getClass());
	
	protected Gson gson = null;
	
	protected MultipleOutputs<NullWritable, Text> multipleOutputs = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		this.gson = new GsonBuilder().serializeSpecialFloatingPointValues()
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}
	
	/**
	 * 统一输入数据的格式
	 * @param inputRecord
	 * @return
	 */
	public abstract Map<String, Object> extractInputRecord(String inputRecord);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//防止文本文件抽取Map缺少指定列值抛错
		Map<String, Object> original = extractInputRecord(value.toString());
		if (null == original || original.isEmpty()) return;
		if ("sourceFile".equalsIgnoreCase(String.valueOf(original.get("sourceFile")))) return;
		if (!original.containsKey("insertTime")) {
			original.put("insertTime", DateFormatter.TIME.get().format(new Date()));
		}
		try {
			for (Map.Entry<String, Object> entry : original.entrySet()) {
				Object realValue = entry.getValue();
				if (ValidateUtils.isAllHalf(realValue)) {
					original.put(entry.getKey(), ValidateUtils.ToDBC(realValue));
				}
			}
			Map<String, Object> correct = new HashMap<String, Object>();
			Map<String, Object> incorrect = new HashMap<String, Object>();
			handle(original, correct, incorrect);
			if (!correct.isEmpty()) {
				String id = IDGenerator.generateByMapValues(correct, 
						"insertTime", "updateTime", "sourceFile", "inputPerson");
				correct.put("_id", id);
				multipleOutputs.write(NullWritable.get(), new Text(gson.toJson(correct)), "correct");
			}
			if (!incorrect.isEmpty()) {
				multipleOutputs.write(NullWritable.get(), new Text(gson.toJson(incorrect)), "incorrect");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 处理原始数据，清理出正确数据与不正确数据
	 * @param original 原始数据
	 * @param correct 正确数据
	 * @param incorrect 不正确数据
	 */
	public abstract void handle(Map<String, Object> original, Map<String, Object> correct,
			Map<String, Object> incorrect);
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
	}

}
