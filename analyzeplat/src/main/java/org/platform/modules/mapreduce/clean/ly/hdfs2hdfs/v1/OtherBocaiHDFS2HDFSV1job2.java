package org.platform.modules.mapreduce.clean.ly.hdfs2hdfs.v1;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSJob;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSMapper;
import org.platform.modules.mapreduce.base.BaseHDFS2HDFSV1Mapper;
import org.platform.modules.mapreduce.clean.ly.cleanTool.BocaiTool;
import org.platform.modules.mapreduce.clean.util.CleanUtil;
/**
 * 博彩第二次清洗
 * @author Administrator
 *
 */
public class OtherBocaiHDFS2HDFSV1job2 extends BaseHDFS2HDFSJob{

	@Override
	public Class<? extends BaseHDFS2HDFSMapper> getMapperClass() {
		return OtherBocaiHDFS2HDFSMapper.class;
	}
	
	public static void main(String[] args) {
//		 args = new
//		 String[]{"","hdfs://192.168.0.10:9000/elasticsearch_clean_1/bocai2","hdfs://192.168.0.10:9000/elasticsearch_clean_1/bocai3"};
		if (args.length != 3) {
			System.out.println("参数不正确");
			System.exit(2);
		}
		String fss = args[1];
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			String[] args1;
			hdfs = FileSystem.get(URI.create(fss), conf);
			FileStatus[] fs = hdfs.listStatus(new Path(fss));
			Path[] listPath = FileUtil.stat2Paths(fs);
			int exitCode = 0;
			for (Path p : listPath) {
				if (!p.getName().equals("_SUCCESS")) {
					args1 = new String[] { args[0], p.toString(), args[2] + "/" + p.getName() };
					System.out.println("执行文件--------------->>>>>>>>>>>>>" + p.toString());
					exitCode = ToolRunner.run(new OtherBocaiHDFS2HDFSV1job2(), args1);
				}
			}
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
class OtherBocaiHDFS2HDFSMapper extends BaseHDFS2HDFSV1Mapper {

	@Override
	public void handle(Map<String, Object> original,
			Map<String, Object> correct, Map<String, Object> incorrect) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.putAll(original);
		// 替换空白字符
		CleanUtil.replaceSpace(original);
		//判断是否是对的数据
		boolean flagOne = false;
		//将手机字段放入cnote（注：数据已经清洗过一次）
		if(original.containsKey("mobilePhone")){
				String mobilePhoneValue = (String) original.get("mobilePhone");
				BocaiTool.addCnote(original,mobilePhoneValue,"mobilePhone");
				original.put("mobilePhone", "NA");
		}
		//将email中的值放入cnote（注：数据已经清洗过一次）
		if(original.containsKey("email")){
			String emailValue = (String) original.get("email");
			BocaiTool.addCnote(original, emailValue, "email");
			original.put("email", "NA");
		}
		//对余额处理
		if(original.containsKey("balance")){
			String balanceValue = (String) original.get("balance");
			if(CleanUtil.matchChinese(balanceValue) || CleanUtil.matchEnglish(balanceValue)){
				BocaiTool.addCnote(original, balanceValue,"balance");
				original.put("balance", "0");
			}
		}
		
		//判断QQ号
		if(original.containsKey("qqNum")){
			String qqValue = (String)original.get("qqNum");
			if(CleanUtil.matchQQ(qqValue)){
				String findQQ = BocaiTool.cleanFile(original, CleanUtil.QQRex, "qqNum", qqValue);
				if(findQQ!=null){
					BocaiTool.addCnote(original, (String) original.get("qqNum"),"qqNum");
					original.put("qqNum", findQQ);
					flagOne=true;
				}
			}else{
				BocaiTool.addCnote(original,qqValue,"qqNum");
				original.put("qqNum", "NA");
			}
		}
		//组合判断的开关
		boolean flagTwo = false; 
		//真实姓名+账号 	或 	 账号 +密码
		if(!flagOne && !flagTwo  && original.containsKey("account") && (original.containsKey("name") || original.containsKey("password"))){
			String nameValue = (String) original.get("name");
			String accountValue = (String) original.get("account");
			String passwordValue = (String) original.get("password");
			if(!"測試帳號".equals(nameValue) &&(CleanUtil.matchChinese(nameValue) || CleanUtil.matchNumAndLetter(passwordValue))&& CleanUtil.matchNumAndLetter(accountValue)){
				flagTwo =true;
			}
		}
		//姓名+银行信息
		if(!flagOne && !flagTwo && original.containsKey("name") && (original.containsKey("cardNo") || original.containsKey("bank"))){
			String nameValue = (String) original.get("name");
			String cardNoValue = (String) original.get("cardNo");
			String bankValue = (String) original.get("bank");
			if(!"測試帳號".equals(nameValue) &&CleanUtil.matchChinese(nameValue) && (CleanUtil.matchNum(cardNoValue) || CleanUtil.matchChinese(bankValue))){
				flagTwo =true;
			}
		}
		//姓名+用户名username
		if(!flagOne && !flagTwo && original.containsKey("name") && original.containsKey("username")){
			String nameValue = (String) original.get("name");
			String usernameValue = (String) original.get("username");
			if(!"測試帳號".equals(nameValue) && CleanUtil.matchChinese(nameValue) && (CleanUtil.matchChinese(usernameValue) || CleanUtil.matchNumAndLetter(usernameValue))){
				flagTwo =true;
			}
		}
		
		//身份证
		if(!flagOne && !flagTwo && original.containsKey("idCard")){
			String idCardValue = (String) original.get("idCard");
			if(CleanUtil.matchIdCard(idCardValue)){
				flagTwo =true;
			}
		}
		//IP地址
		if(!flagOne && !flagTwo && (original.containsKey("registeredIp") || original.containsKey("loginIP"))){
			String registeredIpValue = (String) original.get("registeredIp");
			String loginIPValue = (String) original.get("loginIP");
			if(CleanUtil.matchIP(registeredIpValue) || CleanUtil.matchIP(loginIPValue)){
				flagTwo =true;
			}
		}
		if(flagOne || flagTwo){
			correct.putAll(original);
		}else{
			incorrect.putAll(map);
		}
	}
	
}
