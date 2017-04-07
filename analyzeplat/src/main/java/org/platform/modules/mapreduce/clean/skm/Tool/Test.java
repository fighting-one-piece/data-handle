package org.platform.modules.mapreduce.clean.skm.Tool;

public class Test {
	 public static void main(String[] args) {
		 String regex3 = "^(part)+.*$";
			String  name="part-r-00000";
			if(name.matches(regex3)){
				System.out.println(true);
			}else{
			System.out.println(false);
}
	 }
}
