package org.cisiondata.modules.data.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;





import org.cisiondata.modules.data.dao.DbDAO;
import org.cisiondata.modules.data.service.DbService;
import org.cisiondata.modules.web.WebUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("dbServiceImpl")
public class DbServiceImpl implements DbService {

	@Autowired
	private DbDAO dbDao = null;

	@Override
	public  Map<String,Object> readDbNames() throws Exception {
		List<Map<String, Object>> databases = dbDao.readDbName();
		String count = WebUtils.getCurrentUser().getDirectory();
//		String count="skm";
		List<String> dbNames=new ArrayList<String>();
		for (Map<String, Object> map : databases) {
			Object o=map.get("Database");
			if(o!=null){
				if(o.toString().contains(count)){
					dbNames.add(o.toString());
				}
			}
		}
		Map<String,Object> dbnames=new HashMap<String, Object>();
		dbnames.put("dbnames",dbNames );
		return dbnames;
	}

	@Override
	public  Map<String,Object> readTableNames(String dbName) throws Exception {
		List<String> tables=dbDao.readTablename(dbName);
		if(tables==null){
			return null;
		}
		Map<String,Object> map=new HashMap<String, Object>();
		map.put("tables", tables);
		return map;
	}

	@Override
	public  Map<String,Object> readColumns(String table) throws Exception {
		List<String> columns=dbDao.readTableColumns(table);
		if(columns==null){
			return null;
		}
		Map<String,Object> map=new HashMap<String, Object>();
		map.put("columns", columns);
		return map;
	}

	public  Map<String,Object> readDatas(String table, String type) throws Exception {
		if (type==null || type.equals("0")) type="1";
		List<Map<String, Object>> list=	dbDao.readDatas(table, type);
		if(list==null){
			return null;
		}
		Set<String> columns=list.get(0).keySet();
		List<List<Object>> datas=new ArrayList<List<Object>>();
		for (Map<String, Object> map : list) {
			List<Object> data=new ArrayList<Object>();		
			for (String string : map.keySet()) {
				data.add(map.get(string));
			}
			datas.add(data);
		}
		Map<String,Object> map=new HashMap<String, Object>();
		map.put("columns",columns );
		map.put("data", datas);
		return map;
	}
	
	

}
