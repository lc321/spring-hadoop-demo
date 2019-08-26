package com.test.web;

import com.test.util.HdfsUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@RestController
public class FileListController {

    private static final Logger logger = LogManager.getLogger(FileListController.class);

    @Autowired
    private HdfsUtil hdfsUtil;

    @RequestMapping(value = "/listFileStatus", method = { RequestMethod.GET })
    public Object listFileStatus(HttpServletRequest request) throws IOException{
        logger.info("List FileStatus....");

        String folder = hdfsUtil.getHdfsProperties().getUploadPath();
        return hdfsUtil.lsFile(folder);
    }

}
