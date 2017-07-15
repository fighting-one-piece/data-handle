package org.cisiondata.modules.dataimport.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.cisiondata.modules.dataimport.service.DatabaseQueryService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("index")
@ResponseBody
public class DatabaseQueryController {
	@Resource(name = "databaseQueryService")
	private DatabaseQueryService databaseQueryService = null;
   /**数据库查询*/
	@RequestMapping(value="/toIndexPage",method=RequestMethod.GET)
	public ModelAndView toIndexPage() {
		Map<String, Object> map = new HashMap<String, Object>();
		List<String> list = databaseQueryService.findDbName();
		map.put("dbnames", list);
		ModelAndView modelAndView = new ModelAndView("dataweb", map);
		return modelAndView;
	}

	
	/**数据库表名*/
	@RequestMapping(value="/load",method=RequestMethod.GET)
	   public Map<String, Object> CheckLogin(String dbname){
		     Map<String,Object> map = new HashMap<String,Object> ();
				List<String> list = databaseQueryService.findTbName(dbname);
				map.put("tbnames", list);
				return map;     
	   }
	
	/**数据表100条数据*/
	@RequestMapping(value="/head",method=RequestMethod.GET)
	public Map<String, Object> CheckLogin(String dbname, String tbname) {
		Map<String, Object> map = new HashMap<String, Object>();
		List<Map<Object, Object>> list = databaseQueryService.SelectData(tbname, dbname);
		int size = list.size();
		Map<Object, Object> columnNameMap = list.get(size - 1);
		list.remove(size - 1);
		@SuppressWarnings("unchecked")
		List<String> columnNames = (List<String>) columnNameMap.get("columnNameMap");
		map.put("headnames", columnNames);
		map.put("columnNames", list);
		return map;
	}
}
