package org.platform.modules.mapreduce.clean.lx.function.function;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
/**
 * html2txt
 * @author lixin
 */
public class html2Txt {
	
	public static void convertFile (String path) {
		File file = new File(path);
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int i = 0, len = files.length; i < len; i++) {
				convertFile(files[i].getPath());
			}
		} else if (file.isFile()) {
			String fileName = file.getName();
			String txtFileName = fileName.substring(0, fileName.lastIndexOf(".")) + ".txt";
			html2txt(file.getPath(), file.getParent() + File.separator + txtFileName);
		}
	}
	
	public static void html2txt(String htmlPath, String txtPath) {
		OutputStream out = null;
		BufferedWriter bw = null;
		try {
			StringBuilder sb = new StringBuilder();
			Document document = Jsoup.parse(new File(htmlPath), "utf-8");
			Elements tableElements = document.select("table");
			if (tableElements.size() == 0) return;
			Elements trElements = tableElements.select("tr");
			Iterator<Element> trIterator = trElements.iterator();
			while (trIterator.hasNext()) {
				Element trElement = trIterator.next();
				Elements tdElements = trElement.select("td");
				Iterator<Element> tdIterator = tdElements.iterator();
				while (tdIterator.hasNext()) {
					Element tdElement = tdIterator.next();
					sb.append(tdElement.text()).append(",");
				}
				sb.append("\n");
			}
			System.out.println(sb.toString());
			out = new FileOutputStream(new File(txtPath));
			bw = new BufferedWriter(new OutputStreamWriter(out));
			bw.write(sb.toString());
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != bw) bw.close();
				if (null != out) out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		convertFile("C:\\Users\\woek\\Desktop\\新建文件夹");
	}
	
}
