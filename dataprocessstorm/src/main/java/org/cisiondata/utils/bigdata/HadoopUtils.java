package org.cisiondata.utils.bigdata;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

public class HadoopUtils extends AbstrUtils {
	
	public static final String HDFS_DATA_WAREHOUSE = "hdfs://host-10:9000/warehouse_data";

	public static final String HDFS_USER_WAREHOUSE = "hdfs://host-10:9000/user/dataplat";
	
	public static FileSystem getFileSystem() {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(URI.create(HDFS_DATA_WAREHOUSE), configuration);
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
		}
		return fileSystem;
	}
	
	public static FileSystem getFileSystem(String hdfsPath) {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
		}
		return fileSystem;
	}

}
