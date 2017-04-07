package org.platform.modules.mapreduce.base;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseHDFS2ESV1Reducer extends Reducer<Text, Text, NullWritable, Text> {
	
	public Logger LOG = LoggerFactory.getLogger(BaseHDFS2ESV1Reducer.class);
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), values.iterator().next());
	}

}
