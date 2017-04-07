package org.platform.modules.mapreduce.clean.xx.fileutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ReadFile {
	public static void main(String[] args) {
		try {
			FileReader fr = new FileReader("E:\\车主.txt");
			BufferedReader br = new BufferedReader(fr);
			
			FileWriter writer = new FileWriter("F:\\车主.txt");
            BufferedWriter bw = new BufferedWriter(writer);
			String str = "";
			while ((str=br.readLine())!=null) {
				String[] array = str.split(",");
				String txt = "";
				if ((array.length)<4&&(array.length)>2) {
					txt = str+""+br.readLine();
					System.out.println(txt);
					bw.newLine();
					bw.write(txt);
				}else {
					bw.newLine();
					bw.write(str);
				}
			}
			bw.close();
			writer.close();
			br.close();
			fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
