package org.cisiondata.modules.address;

import java.net.URL;
import java.util.Iterator;
import java.util.List;

import org.cisiondata.modules.address.entity.AdministrativeDivision;
import org.cisiondata.modules.address.service.IAdministrativeDivisionService;
import org.cisiondata.utils.http.HttpUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:spring/applicationContext.xml"})
public class AdministrativeDivisionTest {

	@Autowired
	private IAdministrativeDivisionService administrativeDivisionService = null;
	
	@Test
	public void testInsertAdministrativeDivision() {
		administrativeDivisionService.insertAdministrativeDivisionFromParser();
	}
	
	@Test
	public void testInsertinsertAdministrativeDivisionDictionary() {
		administrativeDivisionService.insertAdministrativeDivisionDictionary();
	}
	
	@Test
	public void testReadAdministrativeDivisionsByParentId() {
		List<AdministrativeDivision> administrativeDivisions = administrativeDivisionService
				.readAdministrativeDivisionsByParentCode(AdministrativeDivision.ROOT);
		for (int i = 0, len = administrativeDivisions.size(); i < len; i++) {
			AdministrativeDivision administrativeDivision = administrativeDivisions.get(i);
			System.out.println(administrativeDivision.getRegion());
			List<AdministrativeDivision> sAdministrativeDivisions = administrativeDivisionService
					.readAdministrativeDivisionsByParentCode(administrativeDivision.getCode());
			for (int j = 0, jlen = sAdministrativeDivisions.size(); j < jlen; j++) {
				AdministrativeDivision sAdministrativeDivision = sAdministrativeDivisions.get(j);
				System.out.println(sAdministrativeDivision.getRegion());
				
			}
		}
	}
	
	@Test
	public void testJsoupParser() {
		String url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/13/04/81/130481207.html";
		try {
//			Document document = Jsoup.connect(url).get();
			Document document = Jsoup.parse(new URL(url).openStream(), "GBK", url);
			Elements elements = document.select("tr.villagetr");
			Iterator<Element> iterator = elements.iterator();
			while (iterator.hasNext()) {
				Element element = iterator.next();
				String content = element.text();
				String[] kv = content.split(" ");
				System.out.println("r:"+kv[0] + ":" + kv[2]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testJsonParser01() {
		String url = "http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html";
		try {
			Document document = Jsoup.connect(url).get();
			Elements elements = document.select("p.MsoNormal");
			Iterator<Element> iterator = elements.iterator();
			while (iterator.hasNext()) {
				Element element = iterator.next();
				System.out.println(element.text());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testHttpParser() {
//		String url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/13/04/81/130481207.html";
		String url = "http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html";
		System.out.println(HttpUtils.sendGet(url));
	}
	
}
