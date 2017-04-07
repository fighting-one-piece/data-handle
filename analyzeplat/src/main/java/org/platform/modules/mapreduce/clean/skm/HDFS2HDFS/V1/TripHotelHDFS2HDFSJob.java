package org.platform.modules.mapreduce.clean.skm.HDFS2HDFS.V1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.util.CleanUtil;

public class TripHotelHDFS2HDFSJob extends BaseHDFS2HDFSJob {
	
	public static void main(String[] args) {
		
		try {
			long timeStar = System.currentTimeMillis();
			int exitCode = 0;
			
			  int index = 0;
			  for (index = 1; index <= 11; index++) {
				  for (int i = 0; i <=9; i++) { 
					  if(i < 10){
				args = new String[] {"","hdfs://192.168.0.10:9000/elasticsearch_original/trip/hotel/105/records-"+ index + "-m-0000" + i,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/trip/hotel/105/records-"+ index + "-m-0000" + i };
					  }else{
				args = new String[] {"","hdfs://192.168.0.10:9000/elasticsearch_original/trip/hotel/105/records-"+ index + "-m-000" + i,
							"hdfs://192.168.0.10:9000/elasticsearch_clean_1/trip/hotel/105/records-"+ index + "-m-000" + i };
					  }		  
				  
				exitCode = ToolRunner.run(new TripHotelHDFS2HDFSJob(), args); 
			  }
		  }
			long timeEnd = System.currentTimeMillis();
			SimpleDateFormat formatter = new SimpleDateFormat("mm分ss秒");
			Date date = new Date(timeEnd - timeStar);
			System.out.println("用时--->" + formatter.format(date));
			System.exit(exitCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Class<? extends BaseHDFS2HDFSV1Mapper> getMapperClass() {
		return HotelHDFS2HDFSMapper.class;
	}

}

class HotelHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {
	@Override
	public void handle(Map<String, Object> original, Map<String, Object> correct, Map<String, Object> incorrect) {
		original = CleanUtil.replaceSpace(original);
        Map<String,Object>map=new HashMap<String,Object>();
        map.putAll(original);
		int size = 4;
		boolean flag = false;
		// String regAddress = "^.*[\u4e00-\u9fa5]{1,}.*$";

		// 判断身份证
		if (original.containsKey("idCard")) {
			String Card = ((String) original.get("idCard")).replace("-", "").replace("(","").replace(")", "");
			if (CleanUtil.matchIdCard(Card)) {
				CleanIdCard(original, Card, "idCard");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("linkIdCard"))
						continue;
					if (CleanUtil.matchIdCard(((String) entry.getValue()))) {
						String m1 = (String) entry.getValue();
						String newIdCard = (String) original.get("idCard");
						entry.setValue(newIdCard);
						CleanIdCard(original, m1, "idCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(Card, "idCard", original);
				}
			}
		}

		// 判断同住人身份证
		if (original.containsKey("linkIdCard")) {
			String ICard = ((String) original.get("linkIdCard")).replace("-", "").replace("(","").replace(")", "");
			if (CleanUtil.matchIdCard(ICard)) {
				CleanIdCard(original, ICard, "linkIdCard");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("idCard"))
						continue;
					if (CleanUtil.matchIdCard(((String) entry.getValue()))) {
						String m1 = (String) entry.getValue();
						String newIdCard = (String) original.get("linkIdCard");
						entry.setValue(newIdCard);
						CleanIdCard(original, m1, "linkIdCard");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(ICard, "linkIdCard", original);
				}
			}
		}
		// 判断入住人手机
		if (original.containsKey("guestPhone")) {
			String phone = ((String) original.get("guestPhone")).replace("-", "").replace("(","").replace(")", "");
			if (CleanUtil.matchCall(phone) || CleanUtil.matchPhone(phone)) {
				CleanPhone(original, phone, "guestPhone");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("guestTel") || entry.getKey().equals("hotelPhone")
							|| entry.getKey().equals("fax")|| entry.getKey().equals("password")
							 ||  entry.getKey().equals("idCard") || entry.getKey().equals("linkIdCard"))
						continue;
					if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
							|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
						String m1 = (String) entry.getValue();
						String ClearPhone = (String) original.get("guestPhone");

						entry.setValue(ClearPhone);
						CleanPhone(original, m1, "guestPhone");
						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(phone, "guestPhone", original);
				}
			}
		}
          
		// 判断移动电话
		if (original.containsKey("guestTel")) {
			String guest = ((String) original.get("guestTel")).replace("-", "").replace("(","").replace(")", "");
			if (CleanUtil.matchPhone(guest)) {
				CleanPhone(original, guest, "guestTel");
				flag = true;
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
							|| entry.getKey().equals("guestPhone") || entry.getKey().equals("hotelPhone")
							|| entry.getKey().equals("fax")|| entry.getKey().equals("password")
							||  entry.getKey().equals("idCard") || entry.getKey().equals("linkIdCard"))
						continue;
					if (CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
						String m1 = (String) entry.getValue();
						String ClearguestTel = (String) original.get("guestTel");

						entry.setValue(ClearguestTel);
						CleanPhone(original, m1, "guestTel");

						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(guest, "guestTel", original);
				}
			}
		}
		 //判断酒店电话
				if (original.containsKey("hotelPhone")) {
					String phone = ((String) original.get("hotelPhone")).replace("-", "").replace("(","").replace(")", "");
					if (CleanUtil.matchCall(phone) || CleanUtil.matchPhone(phone)) {
						CleanPhone(original, phone, "hotelPhone");
						flag = true;
					} else {
						boolean cleanFlag = false;
						for (Entry<String, Object> entry : original.entrySet()) {
							if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
									|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime")
									|| entry.getKey().equals("guestTel") || entry.getKey().equals("guestPhone")
									|| entry.getKey().equals("fax")|| entry.getKey().equals("password")
									||  entry.getKey().equals("idCard") || entry.getKey().equals("linkIdCard"))
								continue;
							if (CleanUtil.matchCall(((String) entry.getValue()).replaceAll("[(]|[)]", "-"))
									|| CleanUtil.matchPhone(((String) entry.getValue()).replaceAll("[(]|[)]", ""))) {
								String m1 = (String) entry.getValue();
								String ClearPhone = (String) original.get("hotelPhone");

								entry.setValue(ClearPhone);
								CleanPhone(original, m1, "hotelPhone");
								cleanFlag = true;
								flag = true;
								break;
							}
						}
						if (!cleanFlag) {
							size++;
							Scrap(phone, "hotelPhone", original);
						}
					}
				}

		// 判断email
		if (original.containsKey("email")) {
			String email = (String) original.get("email");
			if (CleanUtil.matchEmail((String) original.get("email"))) {
				flag = true;
				CleanEmail(original, email, "email");
			} else {
				boolean cleanFlag = false;
				for (Entry<String, Object> entry : original.entrySet()) {
					if (entry.getKey().equals("_id") || entry.getKey().equals("sourceFile")
							|| entry.getKey().equals("insertTime") || entry.getKey().equals("updateTime"))
						continue;
					if (CleanUtil.matchEmail((String) original.get("email"))) {
						String m1 = (String) entry.getValue();
						String Newemail = (String) original.get("email");
						entry.setValue(Newemail);
						CleanEmail(original, m1, "email");

						cleanFlag = true;
						flag = true;
						break;
					}
				}
				if (!cleanFlag) {
					size++;
					Scrap(email, "email", original);
				}
			}
		}
		
//		//判断邮编		
		if (original.containsKey("zipCode")) {
			String Card = ((String) original.get("zipCode")).replace("-", "").replace("(", "").replace(")", "");
			String red = "^[1-9]\\d{5}(?!\\d)$";
			if (Card.matches(red)) {
				flag = true;
			} else {
				if (StringUtils.isNotBlank(Card) && !"NA".equals(Card)) {
					if (original.containsKey("cnote")) {
						String cnote = ((String) original.get("cnote")).equals("NA") ? ""
								: (String) original.get("cnote");
						original.put("cnote", Card + cnote);
					} else {
						original.put("cnote", Card);
					}
				}
				original.remove("zipCode");
			}
		}
		
		//账号与密码同时存在
		 if(!flag){
		if(original.containsKey("account")&&original.containsKey("password")){
			String account=(String)original.get("account");
			String password=(String)original.get("password");
			   if(!("NA".equals(password))&&!(password.equals(""))){
				   if(!("NA".equals(account))&&!(account.equals(""))){
					   flag=true;
				   }
			   }
							
			}	
		 }
		 //姓名地址同时存在
		 if(!flag){
		if(original.containsKey("name")&&original.containsKey("homeAddress")){
			String name=(String)original.get("name");
			String homeAddress=(String)original.get("homeAddress");
			   if(!("NA".equals(homeAddress))&&!(homeAddress.equals(""))&&CleanUtil.matchChinese(homeAddress)){
				   if(!("NA".equals(name))&&!(name.equals(""))&&!CleanUtil.matchNum(name)){
					   flag=true;
				   }
			   }
							
			}
		 }
		
		//酒店姓名地址同时存在
		 if(!flag){
		if(original.containsKey("hotelname")&&original.containsKey("hotelAddress")){
			String hname=(String)original.get("hotelname");
			String hotelAddress=(String)original.get("hotelAddress");
			   if(!("NA".equals(hotelAddress))&&!(hotelAddress.equals(""))&&CleanUtil.matchChinese(hotelAddress)){
				   if(!("NA".equals(hname))&&!(hname.equals(""))&&!CleanUtil.matchNum(hname)){
					   flag=true;
				   }
			   }
							
			}
		 }
		 
		//判断除NA之外的有效字段
			int corriSize = 0;
			for (Entry<String, Object> entry : original.entrySet()) {
				if (!"NA".equals((String) entry.getValue()) && !"".equals((String) entry.getValue())
						&& !"null".equals((String) entry.getValue()) && !"NULL".equals((String) entry.getValue())) {
					corriSize++;
				}
			}
			if (size <= 5 && corriSize <= 5) {
				flag = false;
			}
		 
		if (flag) {
			correct.putAll(replaceKey(original));
		} else {
			incorrect.putAll(map);
		}
	}

	private Map<String, Object> replaceKey(Map<String, Object> original) {

		if (original.containsKey("name")) {
			original.put("nameAlias", original.get("name"));
		}

		if (original.containsKey("linkName")) {
			original.put("linkNameAlias", original.get("linkName"));
		}
		if (original.containsKey("hotelName")) {
			original.put("hotel", original.get("hotelName"));
			original.remove("hotelName");
		}

		if (original.containsKey("guestPhone")) {
			original.put("mobilePhone", original.get("guestPhone"));
			original.remove("guestPhone");
		}

		if (original.containsKey("hotelPhone")) {
			original.put("hotelTelePhone", original.get("hotelPhone"));
			original.remove("hotelPhone");
		}
		if (original.containsKey("homeAddress")) {
			original.put("address", original.get("homeAddress"));
			original.remove("homeAddress");
		}

		if (original.containsKey("guestTel")) {
			original.put("guestMobilePhone", original.get("guestTel"));
			original.remove("guestTel");
		}

		if (original.containsKey("nationality")) {
			original.put("country", original.get("nationality"));
			original.remove("nationality");
		}
		if (original.containsKey("nation")) {
			if (original.containsKey("country")) {
				String value = (String) original.get("country") + (String) original.get("nation");
				original.put("country", value);
			} else {
				original.put("country", original.get("nation"));
			}
			original.remove("nation");
		}

		if (original.containsKey("national")) {
			original.put("nationality", original.get("national"));
			original.remove("national");
		}
		return original;
	}

	private void CleanIdCard(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();

		Pattern pat = Pattern.compile(CleanUtil.idCardRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buffer.append(listFiled.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = Filed.replaceAll(CleanUtil.idCardRex, ""); // 将不匹配的元素取出来

       if (StringUtils.isNotBlank(supersession)&& !"NA".equals(supersession)) {
			
			if(original.containsKey("cnote")){
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote+supersession);
			}else{
				original.put("cnote", supersession);
			}
			
		}
		original.put(FiledName, toBuffer);
	}
	

	/**
	 * 
	 * @param original
	 * @param email
	 * @param string
	 */
	private void CleanEmail(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();

		Pattern pat = Pattern.compile(CleanUtil.emailRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
		}
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buffer.append(listFiled.get(i)); // 将正确的数据放到一个字符串中
			} else {
				buffer.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buffer.toString();
		String supersession = Filed.replaceAll(CleanUtil.emailRex, ""); // 将不匹配的元素取出来
if (StringUtils.isNotBlank(supersession)&& !"NA".equals(supersession)) {
			
			if(original.containsKey("cnote")){
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote+supersession);
			}else{
				original.put("cnote", supersession);
			}
			
		}
		original.put(FiledName, toBuffer);
	}

	/**
	 * 
	 * @param original
	 * @param Filed
	 *            字段值
	 * @param FiedName
	 *            字段名
	 */
	private void CleanPhone(Map<String, Object> original, String Filed, String FiledName) {
		List<Object> listFiled = new ArrayList<Object>();
		StringBuffer buf = new StringBuffer();
		Pattern pat = Pattern.compile(CleanUtil.phoneRex);
		Matcher M = pat.matcher(Filed);
		while (M.find()) {
			listFiled.add(M.group());
		}
		String supersession = Filed.replaceAll(CleanUtil.phoneRex, ""); // 将不匹配的元素取出来

		pat = Pattern.compile(CleanUtil.callRex);
		M = pat.matcher(supersession);
		while (M.find()) {
			listFiled.add(M.group());
		}
		for (int i = 0; i < listFiled.size(); i++) {
			if (i == 0) {
				buf.append(listFiled.get(i));
			} else {
				buf.append("," + listFiled.get(i));
			}
		}
		String toBuffer = buf.toString();
		supersession = supersession.replaceAll(CleanUtil.callRex, ""); // 将不匹配的元素取出来

if (StringUtils.isNotBlank(supersession)&& !"NA".equals(supersession)) {
			
			if(original.containsKey("cnote")){
				String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
				original.put("cnote", cnote+supersession);
			}else{
				original.put("cnote", supersession);
			}
			
		}
		original.put(FiledName, toBuffer);
	}


	/**
	 * 将废值替换为NA
	 */
	public void Scrap(String Filed, String FiledName, Map<String, Object> original) {
		if (!"NA".equals(Filed) && StringUtils.isNotBlank(Filed)) {
			  if(original.containsKey("cnote")){
				  String cnote = ((String) original.get("cnote")).equals("NA") ? "" : (String) original.get("cnote");
					original.put("cnote", cnote + Filed);
			  }else{
				  original.put("cnote", Filed);
			  }
		}
		original.put(FiledName, "NA");
	}

}
