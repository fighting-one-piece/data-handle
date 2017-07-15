package org.cisiondata.modules.dataimport.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.dataimport.service.IMYSQL2HDFSService;
import org.cisiondata.modules.web.WebUtils;
import org.cisiondata.utils.exception.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import freemarker.template.Configuration;
import freemarker.template.Template;

/**
 * @author xiexin
 *
 */
@Service("mysql2hdfsService")
public class MYSQL2HDFSServiceImpl implements IMYSQL2HDFSService {
	Logger LOG = LoggerFactory.getLogger(MYSQL2HDFSServiceImpl.class);
	
	private SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
	private SimpleDateFormat sdf1 = new SimpleDateFormat("YYYYMMddHHmmss");

	@Override
	public String createJson(String database,String table,String index ,String type,String[] mysqlColumns,String[] hdfsColumns) {
		try {
			String inputPerson = WebUtils.getCurrentAccout();
			String directory = WebUtils.getDirectory();
			if (null == inputPerson || null == directory) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
			String mysqlC = "";
			for (int i = 0; i < mysqlColumns.length; i++) {
				mysqlC += mysqlColumns[i] + ",";
			}
			mysqlC = mysqlC.substring(0, mysqlC.length() - 1);
			String esColumn = "";
			for (int i = 0; i < hdfsColumns.length; i++) {
				esColumn += "{\"name\":\"" + hdfsColumns[i] + "\",\"type\":\"string\"},";
			}
			esColumn = esColumn.substring(0, esColumn.length() - 1);
			// 创建插值的map
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("mysqlColumn", mysqlC);
			map.put("inputPerson", inputPerson);
			map.put("database", database);
			map.put("table", table);
			map.put("hdfsColumn", esColumn);
			map.put("type", type);
			map.put("index", index);
			map.put("date", sdf.format(new Date()));

			@SuppressWarnings("deprecation")
			Configuration config = new Configuration();
			config.setDefaultEncoding("UTF-8");
			String templatePath = MYSQL2HDFSServiceImpl.class.getClassLoader().getResource("upload").getPath();
			config.setDirectoryForTemplateLoading(new File(templatePath));
			// 创建一个模板对象
			Template template = config.getTemplate("mysql_to_hdfs.ftl");
			// 执行插值，并输出到指定的输出流中
			String end = sdf1.format(new Date());
			template.process(map, new FileWriter("/home/"+directory+"/Datax/datax/job/"+ database + table + end +".json"));
//			// 控制台输出结果
//			template.process(map, new OutputStreamWriter(System.out));

			return "创建成功！！文件名："+ database + table +".json";
		} catch (Exception e) {
			e.printStackTrace();
			return "创建失败！！！";
		}
	}

	@Override
	public String executeOneJson(String path) {
		try {
			Process process = null;
			BufferedReader in = null;
			// 控制台输出结果
			process = Runtime.getRuntime().exec("python /home/ym/Datax/datax/bin/datax.py " + path);
			int iWaitFor = process.waitFor();
			if (iWaitFor != 0) {
				// 出错写log
			}
			in = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = null;
			StringBuilder builder = new StringBuilder();
			while ((line = in.readLine()) != null) {
				builder.append(line+"\n");
			}
			System.out.println(builder.toString());
			return builder.toString();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<Map<String,Object>> readJsonList() {
		String directory = WebUtils.getDirectory();
		if (null == directory) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
//		File f = new File("/home/"+directory+"/Datax/datax/job");
		File f = new File("C:/Users/admin/Desktop");
		File[] t = f.listFiles();
		if (null == t || t.length == 0) return new ArrayList<Map<String,Object>>();
		for (int i = 0; i < t.length; i++) {
			if (!t[i].isFile()) continue;
			String fileName = t[i].getName();
			if (!fileName.endsWith(".json")) continue;
			Map<String,Object> map = new HashMap<String, Object>();
			map.put("fileName", fileName);
			map.put("lastModefied", t[i].lastModified());
			list.add(map);
		}
		Collections.sort(list, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				if (((Long)o1.get("lastModefied")) > ((Long)o2.get("lastModefied"))){
					return -1;
				} else {
					return 1;
				}
			}
		});
		return list;
	}
	
	@Override
	public String readJson(String fileName) throws BusinessException{
		String directory = WebUtils.getDirectory();
		if (null == directory) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
//		File f = new File("/home/"+directory+"/Datax/datax/job/"+fileName);
		File f = new File("C:/Users/admin/Desktop/"+fileName);
		
		if (!f.exists())throw new BusinessException(ResultCode.FILE_NOT_EXITED);
		BufferedReader bufferedReader = null;
		String str = "";
		try {
			bufferedReader = new BufferedReader(new FileReader(f));
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				str += line + "\n";
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return str;
	}

	@Override
	public void updateJson(String fileName,String data) throws BusinessException {
		if(StringUtils.isBlank(fileName)) throw new BusinessException(ResultCode.PARAM_NULL);
		String directory = WebUtils.getDirectory();
		if (null == directory) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
//		File f = new File("/home/"+directory+"/Datax/datax/job/"+fileName);
		File f = new File("C:/Users/admin/Desktop/"+fileName);
		if (!f.exists())throw new BusinessException(ResultCode.FILE_NOT_EXITED);
		BufferedWriter br = null;
		data = data == null ? "" : data;
		try {
			br = new BufferedWriter(new FileWriter(f));
			br.write(data);
			br.flush();
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error(e.getMessage(),e);
			}
		}
	}

	@Override
	public void deleteJson(String fileName) throws BusinessException {
		if(StringUtils.isBlank(fileName)) throw new BusinessException(ResultCode.PARAM_NULL);
		String directory = WebUtils.getDirectory();
		if (null == directory) throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
//		File f = new File("/home/"+directory+"/Datax/datax/job/"+fileName);
		File f = new File("C:/Users/admin/Desktop/"+fileName);
		if (!f.exists())throw new BusinessException(ResultCode.FILE_NOT_EXITED);
		f.delete();
	}
}