package com.test.web;

import com.test.util.HdfsUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@RestController
public class DeleteFileController {

    private static final Logger logger = LogManager.getLogger(DeleteFileController.class);

    @Autowired
    private HdfsUtil hdfsUtil;

    @RequestMapping(value = "/deleteFile", method = { RequestMethod.GET })
    public String deleteFile(HttpServletRequest request,@RequestParam("folderName")String folderName,@RequestParam("rmdir")String rmdir) throws IOException{
        logger.info("Delete File....");

        boolean rmdir1 = Boolean.parseBoolean(rmdir);
        String originFoldername = hdfsUtil.getHdfsProperties().getUploadPath() + folderName;
        //删除文件操作
        if(hdfsUtil.rmFile(originFoldername,rmdir1)){
            return "删除成功";
        }
        return "删除失败";
    }

}
