package com.test.web;

import java.io.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.test.util.HdfsUtil;
import com.test.util.ThrowableUtil;

@RestController
public class UploadDownloadController {

	private static final Logger logger = LogManager.getLogger(UploadDownloadController.class);

	@Autowired
	private HdfsUtil hdfsUtil;

	@RequestMapping(value = "/uploadToHdfs", method = { RequestMethod.POST })
	public String uploadToHdfs(HttpServletRequest request, @RequestParam("file") MultipartFile file)
			throws IllegalStateException, IOException {
		logger.info("uploadToHdfs started....");
		if (!file.isEmpty()) {
			logger.info("file.size... " + file.getSize());
			try {
				String originalFilename = file.getOriginalFilename();

				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(originalFilename)));

				out.write(file.getBytes());

				out.flush();
				out.close();

				String destFileName = hdfsUtil.getHdfsProperties().getUploadPath() + originalFilename;

				logger.info("destFileName::: " + destFileName);

				hdfsUtil.uploadFile(new String[] { originalFilename, destFileName });

			} catch (FileNotFoundException e) {
				e.printStackTrace();
				logger.error(ThrowableUtil.getErrorInfoFromThrowable(e));
				return "上传失败，" + e.getMessage();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(ThrowableUtil.getErrorInfoFromThrowable(e));
				return "上传失败, " + e.getMessage();
			}

			return "上传成功";

		}
		return "上传失败，文件为空。";
	}

	@RequestMapping(value = "/downloadFromHdfs", method = { RequestMethod.GET })
	public String downloadFromHdfs(HttpServletRequest request, HttpServletResponse response,@RequestParam("fileName")String fileName)
			throws IllegalStateException, IOException {
		logger.info("downloadFromHdfs started....");
		System.out.println("------------------------------------------------------------");
		String originalFilename = hdfsUtil.getHdfsProperties().getUploadPath() + fileName;
		//String destFileName = localFilePath;
		//hdfs文件流读取文件
		FSDataInputStream in = hdfsUtil.downloadFile(new String[] { originalFilename});
		if(in == null){
			return  "下载失败,文件不存在";
		}

		//设置文件ContentType类型，自动判断下载文件类型
		response.setContentType("multipart/form-data");
		//通知浏览器以下载方式打开
		response.addHeader("Content-type", "appllication/octet-stream");
		response.setHeader("Content-Disposition", "attachment;filename=" + fileName);

		//获取文件输出
		OutputStream out = response.getOutputStream();
		byte[] buffer = new byte[1024];
		int len;
		//循环取出流中的数据
		while((len = in.read(buffer)) != -1){
			out.write(buffer,0,len);
			out.flush();
		}
		out.close();
		System.out.println("------------------------------------------------------------");
		System.out.println("下载结束");

		return "下载成功";
	}

}
