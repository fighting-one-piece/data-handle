package org.platform.modules.mapreduce.base;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHDFS2ESV4Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ESV4Mapper.class);
	
	private boolean isESInputJson = false;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		String esInputJsonString = context.getConfiguration().get("es.input.json", "no");
		isESInputJson = "yes".equalsIgnoreCase(esInputJsonString) ? true : false;
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			context.write(NullWritable.get(), handle(wrapperValue(value)));  
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	protected Text handle(Text value) {
		return value;
	}
	
	private Text wrapperValue(Text value) {
    	if (isESInputJson) {
    		return new Text(value.toString().replaceFirst("\\\"_id\\\"", "\\\"id\\\""));
    	}
    	return value;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
	}

}

