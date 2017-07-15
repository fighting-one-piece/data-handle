package org.cisiondata.modules.data.controller;


import java.util.Map;



import org.cisiondata.modules.data.service.impl.DbServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;


@Controller
public class DataController {
	
	
	@Autowired
	private DbServiceImpl dbService=null;
	@RequestMapping(value = "/data", method = RequestMethod.GET)
	public ModelAndView Upload() {
		return new ModelAndView("/data/data");
	}
	@RequestMapping(value = "/readDatabase", method = RequestMethod.GET)
	public  Map<String,Object>  readDatabase(){
		Map<String,Object> map=null;
			try {
				map=dbService.readDbNames();
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			return map;
	}
	@RequestMapping(value = "/readTables", method = RequestMethod.GET)
	public  Map<String,Object>  readTables(String dbName){
		System.out.println(dbName);
		Map<String,Object> map=null;
			try {
				map=dbService.readTableNames(dbName);
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			return map;
	}
	
	@RequestMapping(value = "/readDatas", method = RequestMethod.GET)
	public Map<String,Object> readDatas(String tableName,String row){
		
		Map<String,Object> map=null;
		try {
			 tableName = new String(tableName.getBytes("iso-8859-1"),"utf-8");
			 System.out.println(tableName);
			 System.out.println(row);
			 map=	dbService.readDatas(tableName, row);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return map;
	}
	
}
