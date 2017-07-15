package org.cisiondata.utils.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.cisiondata.utils.serde.SerializerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);
	
	private static HBaseAdmin admin = null;
	
	private static Connection connection = null;
	
	private static Configuration configuration = null;
	
	static {
		initializeConfig();
		initializeClient();
	}
	
	private static void initializeConfig() {
		System.setProperty("hadoop.home.dir", "F:/develop/hadoop/hadoop-2.7.2");
		System.setProperty("HADOOP_MAPRED_HOME", "F:/develop/hadoop/hadoop-2.7.2");
		configuration = new Configuration();
		/** 与hbase/conf/hbase-site.xml中hbase.master配置的值相同 */
		configuration.set("hbase.master", "host-10:60000");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同 */
		configuration.set("hbase.zookeeper.quorum", "host-10,host-11,host-12,host-13,host-14");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 */
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//configuration = HBaseConfiguration.create(configuration);
	}
	
	private static void initializeClient() {
		try {
			ExecutorService pool = Executors.newCachedThreadPool();
			connection = ConnectionFactory.createConnection(configuration, pool);
			admin = (HBaseAdmin) connection.getAdmin();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/** 创建一张表*/
	public static void creatTable(String tableName, String[] familys) {
		try {
			if (admin.tableExists(tableName)) {
				LOG.info("table "+ tableName + " already exists!");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
				for (int i = 0; i < familys.length; i++) {
					tableDesc.addFamily(new HColumnDescriptor(familys[i]));
				}
				admin.createTable(tableDesc);
				LOG.info("create table " + tableName + " success.");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	/** 表注册Coprocessor*/
	public static void addTableCoprocessor(String tableName, String coprocessorClassName) {
		try {
			admin.disableTable(tableName);
			HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
			htd.addCoprocessor(coprocessorClassName);
			admin.modifyTable(Bytes.toBytes(tableName), htd);
			admin.enableTable(tableName);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/** 统计表行数*/
	public static long rowCount(String tableName) {
		long rowCount = 0;
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
//			scan.setFilter(new KeyOnlyFilter());
			scan.setFilter(new FirstKeyOnlyFilter());
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				rowCount += result.size();
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return rowCount;
	}

	/** 插入一行记录*/
	public static void insertRecord(String tableName, String rowKey, String family, String qualifier, Object value) {
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), SerializerUtils.write(value));
			table.put(put);
			LOG.info("insert recored " + rowKey + " to table " + tableName + " success.");
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
		}
	}
	
	/** 批量插入记录*/
	public static void insertRecords(String tableName, List<Put> puts) {
		HTable table = null;
		try {
			table = (HTable) connection.getTable(TableName.valueOf(tableName));
			table.put(puts);
		} catch (IOException e) {
			LOG.info(e.getMessage(), e);
			try {
				table.flushCommits();
			} catch (Exception e1) {
				LOG.info(e1.getMessage(), e1);
			}
		}
	}
	
	/** 删除一行记录*/
	public static void deleteRecord(String tableName, String... rowKeys) {
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			List<Delete> list = new ArrayList<Delete>();
			Delete delete = null;
			for (String rowKey : rowKeys) {
				delete = new Delete(rowKey.getBytes());
				list.add(delete);
			}
			if (list.size() > 0) {
				table.delete(list);
			}
			LOG.info("delete recoreds " + rowKeys + " success.");
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/** 删除一个列族*/
	public static void deleteFamily(String tableName, String columnName) {
		try {
			admin.deleteColumn(tableName, columnName);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	/** 删除表*/
	public static void deleteTable(String tableName) {
		try {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			LOG.info("delete table " + tableName + " success.");
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} 
	}

	/** 查找一行记录*/
	public static Result getRecord(String tableName, String rowKey) {
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey.getBytes());
			get.setMaxVersions();
			return table.get(get);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName) {
		return getRecords(tableName, null, null, null, null, null);
	}
	
	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName, String family) {
		return getRecords(tableName, family, null, null, null, null);
	}
	
	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName, String family, String qualifier) {
		return getRecords(tableName, family, qualifier, null, null, null);
	}
	
	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName, Filter filter) {
		return getRecords(tableName, null, null, null, null, filter);
	}
	
	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName, String family, String qualifier, 
			String startRow, String stopRow, Filter filter) {
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if (!StringUtils.isBlank(family)) scan.addFamily(Bytes.toBytes(family));
			if (!StringUtils.isBlank(family) && !StringUtils.isBlank(qualifier)) 
				scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			if (!StringUtils.isBlank(startRow)) scan.setStartRow(Bytes.toBytes(startRow));
			if (!StringUtils.isBlank(stopRow)) scan.setStopRow(Bytes.toBytes(stopRow));
			if (null != filter) scan.setFilter(filter);
			return table.getScanner(scan);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
	
	/** 查找所有记录*/
	public static ResultScanner getRecords(String tableName, Scan scan) {
		try {
			HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
			return table.getScanner(scan);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
	
	public static void printRecord(Result result) {
		try {
			List<Cell> cells= result.listCells();
			for (Cell cell : cells) {
				String row = new String(result.getRow(), "UTF-8");
				String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
				String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
				String value = (String) SerializerUtils.read(CellUtil.cloneValue(cell));
				System.out.println("[row:"+row+"],[family:"+family+"],[qualifier:"+qualifier+"],[value:"+value+"]");
			} 
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	public static void printRecords(ResultScanner resultScanner) {
		for (Result result : resultScanner) {
			printRecord(result);
		}
	}
	
	public static void main(String[] args) throws Exception {
//		HBaseUtils.creatTable("user", new String[]{"base", "education"});
//		String rowKey = Calendar.getInstance().getTimeInMillis() + "";
//		HBaseUtils.insertRecord("user", rowKey, "base", "name", "maliu");
//		HBaseUtils.insertRecord("user", rowKey, "base", "age", "21");
//		HBaseUtils.insertRecord("user", rowKey, "education", "school", "ksez");
//		HBaseUtils.insertRecord("user", rowKey, "education", "univercity", "xjdx");
//		HBaseUtils.printRecords(HBaseUtils.getRecords("user"));
		HBaseUtils.printRecords(HBaseUtils.getRecords("student"));
	}

}
