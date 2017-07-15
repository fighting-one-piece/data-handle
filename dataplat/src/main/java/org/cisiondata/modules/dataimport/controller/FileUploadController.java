package org.cisiondata.modules.dataimport.controller;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.cisiondata.modules.abstr.web.ResultCode;
import org.cisiondata.modules.dataimport.entity.PlUploadEntity;
import org.cisiondata.modules.dataimport.service.impl.UploadAndToMysqlImpl;
import org.cisiondata.modules.dataimport.utils.PlUploadUtil;
import org.cisiondata.modules.web.WebUtils;
import org.cisiondata.utils.exception.BusinessException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class FileUploadController {
	@Resource(name = "uploadAndToMysql")
	private UploadAndToMysqlImpl upLoadAndToMysql = null;

	@RequestMapping(value = "/upload", method = RequestMethod.GET)
	public ModelAndView Upload() {
		return new ModelAndView("/home/home");
	}

	@RequestMapping(value = "/filesUpload", method = RequestMethod.POST)
	@ResponseBody
	public void filesUpload(String separator, PlUploadEntity plupload, HttpServletRequest request,
			HttpServletResponse response) throws BusinessException {
		plupload.setRequest(request);// 手动传入Plupload对象HttpServletRequest属性
		String person = WebUtils.getDirectory();
		if (StringUtils.isBlank(person))
			throw new BusinessException(ResultCode.VERIFICATION_USER_FAIL);
		// 文件存储绝对路径,会是一个文件夹
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date curDate = new Date(System.currentTimeMillis());// 获取当前时间
		String fileStar = formatter.format(curDate).replaceAll("-", "").replaceAll(" ", "").replaceAll(":", "");
		String filesPath = FileUploadController.class.getClassLoader().getResource("upload").getPath() + "file"
				+ fileStar + "/";
		File dir = new File(filesPath);
		upLoadAndToMysql.createDir(filesPath); // 创建目录
		// 开始上传文件
		try {
			PlUploadUtil.upload(plupload, dir);
			upLoadAndToMysql.fileToMySQL(plupload.getName(), filesPath, person, separator); // 导入mysql
			upLoadAndToMysql.deleteFile(filesPath); // 删除文件
		} catch (BusinessException bu) {
			bu.getDefaultMessage();
		} catch (IllegalStateException | IOException e) {
			e.printStackTrace();
			System.out.println("上传出错！");
		}
	}
}