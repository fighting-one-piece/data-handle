package org.platform.modules.mapreduce.base;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHDFS2ES5V1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseHDFS2ES5V1Reducer.class);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long totalLines = 0L;
		for (LongWritable value : values) {
			totalLines = totalLines + value.get();
		}
		LOG.info("produce total lines: " + totalLines);
		context.write(key, new LongWritable(totalLines));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
}

