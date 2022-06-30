//package com.tapdata.tm.file.service;
//
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.gridfs.GridFSBucket;
//import com.mongodb.client.gridfs.GridFSBuckets;
//import com.mongodb.client.gridfs.GridFSDownloadStream;
//import com.mongodb.client.gridfs.GridFSFindIterable;
//import com.mongodb.client.gridfs.model.GridFSFile;
//import com.tapdata.tm.utils.GZIPUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.compress.utils.IOUtils;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.data.mongodb.MongoDatabaseFactory;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.gridfs.GridFsResource;
//import org.springframework.data.mongodb.gridfs.GridFsTemplate;
//import org.springframework.stereotype.Service;
//
//import javax.annotation.Resource;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.io.UnsupportedEncodingException;
//import java.net.URLEncoder;
//import java.nio.charset.StandardCharsets;
//import java.util.Map;
//
///**
// * @author lg<lirufei0808 @ gmail.com>
// * @date 2021/4/19 下午2:34
// * @description
// */
//@Service
//@Configuration
//@Slf4j
//
//public class FileService1 {
//
//  @Resource
//  private MongoDatabaseFactory mongoDatabaseFactory;
//  @Bean
//  public GridFSBucket getGridFSBuckets() {
//    MongoDatabase db = mongoDatabaseFactory.getMongoDatabase();
//    return GridFSBuckets.create(db);
//  }
//
//  private static final String[] FILE_TYPES =
//    new String[] {
//      "jpg", "bmp", "jpeg", "png", "gif", "tif", "pcx", "tga", "exif", "exif", "svg",
//      "psd", "cdr", "pcd", "dxf", "ufo", "eps", "ai", "raw", "WMF", "webp", "icon"
//    };
//
//  @Resource
//  private GridFSBucket gridFSBucket;
//  private GridFsTemplate gridFsTemplate;
//  public FileService1(GridFsTemplate gridFsTemplate){
//    this.gridFsTemplate = gridFsTemplate;
//  }
//
//  public GridFSFindIterable find(Query query){
//    return gridFsTemplate.find(query);
//  }
//
//  /**
//   * 查询
//   * @param query
//   * @return
//   */
//  public GridFSFile findOne(Query query){
//    return gridFsTemplate.findOne(query);
//  }
//
//  /**
//   * 保存文件
//   * @param content   文件流，不能为null
//   * @param filename  文件名，不能为null
//   * @param contentType   文件类型，可以为null
//   * @param metadata      文件元信息，可以为null
//   * @return
//   */
//  public ObjectId storeFile(InputStream content, String filename, String contentType, Map<String, Object> metadata) {
//    Document document = new Document(metadata);
//    return gridFsTemplate.store(content, filename, contentType, document);
//  }
//
//  /**
//   * 读文件
//   * @param gridFSFile
//   * @param outputStream
//   * @return
//   */
//  public long readFileById(GridFSFile gridFSFile, OutputStream outputStream) {
//    GridFsResource resource = gridFsTemplate.getResource(gridFSFile);
//    try {
//      InputStream inputStream = resource.getInputStream();
//      long count = IOUtils.copy(inputStream, outputStream);
//
//      IOUtils.closeQuietly(inputStream);
//
//      return count;
//    } catch (IOException e) {
//      log.error("Read file({}) failed", gridFSFile.getId(), e);
//    }
//    return 0;
//  }
//
//  /**
//   *
//   * @param id
//   * @param response
//   * @return
//   * @throws UnsupportedEncodingException
//   */
//
//  public long readFileById(ObjectId id, HttpServletResponse response) throws UnsupportedEncodingException {
//    GridFSFile gridFSFile = findOne(Query.query(Criteria.where("_id").is(id)));
//    GridFsResource resource = gridFsTemplate.getResource(gridFSFile);
//
//    response.setContentType("application/octet-stream");
//    response.addHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(resource.getFilename(), "UTF-8"));
//    try {
//      InputStream inputStream = resource.getInputStream();
//      long count = IOUtils.copy(inputStream, response.getOutputStream());
//
//      IOUtils.closeQuietly(inputStream);
//
//      return count;
//    } catch (IOException e) {
//      log.error("Read file({}) failed", gridFSFile.getId(), e);
//    }
//    return 0;
//  }
//
//  /**
//   * 读文件
//   * @param id    文件ID
//   * @param outputStream
//   * @return
//   */
//  public long readFileById(ObjectId id, OutputStream outputStream) {
//    return readFileById(findOne(Query.query(Criteria.where("_id").is(id))), outputStream);
//  }
//
//  /**
//   * 删除文件
//   * @param id
//   */
//  public void deleteFileById(ObjectId id) {
//    gridFsTemplate.delete(Query.query(Criteria.where("_id").is(id)));
//  }
//  public void viewImg(ObjectId fileId, HttpServletResponse response) {
//
//    try {
//      GridFSFile file = gridFsTemplate.findOne(Query.query(Criteria.where("_id").is(fileId)));
//      if (null == file) {
//        throw new RuntimeException("ID对应文件不存在");
//      }
//      String fileName = file.getFilename();
//      OutputStream out = response.getOutputStream();
//      //取后缀
//      String filename = file.getFilename();
//      String fileType = filename.substring(filename.lastIndexOf(".") + 1);
//      boolean flag = false;
//      for (String obj : FILE_TYPES) {
//        if (fileType.equalsIgnoreCase(obj)) {
//          flag = true;
//          break;
//        }
//      }
//      if (flag) {
//        log.info("图片类型,进入预览");
//        response.setHeader("Content-disposition", "inline; filename=" + fileName);
//        response.setContentType("image/jpeg");
//        // 不进行压缩的文件大小，单位为bit
//        InputStream in = convertGridFSFile2Resource(file).getInputStream();
//        int contentLength = (int) convertGridFSFile2Resource(file).contentLength();
//        byte[] data = new byte[contentLength];
//        in.read(data);
//        //response here is the HttpServletResponse object
//        response.setContentLength(contentLength);
//        out.write(data);
//        /** 采用压缩方式，则需注释调这段代码-结束 **/
//        out.flush();
//        out.close();
//      } else {
//        log.info("非图片类型,进入下载");
//        //转成GridFsResource类取文件类型
//        response.setContentType("application/octet-stream");
//        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
//        //文件长度
//        response.setHeader("Content-Length", String.valueOf(file.getLength()));
//        try {
//          //IO复制
//          InputStream in = convertGridFSFile2Resource(file).getInputStream();
//          IOUtils.copy(
//            convertGridFSFile2Resource(file).getInputStream(), out);
//        } catch (IOException e) {
//          log.error(e.getMessage());
//        }
//      }
//    } catch (Exception e) {
//      log.warn("download file failed, filp id = {}", fileId);
//    }
//  }
//
//  public void viewImg1(String json, HttpServletResponse response) {
//
//    try {
//
//      String fileName = "task.json.gz";
//      OutputStream out = response.getOutputStream();
//      response.setHeader("Content-disposition", "inline; filename=" + fileName);
//      response.setContentType("application/octet-stream");
//      // 不进行压缩的文件大小，单位为bit
//      byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
//      byte[] gzip = GZIPUtil.gzip(bytes);
//      //response here is the HttpServletResponse object
//      response.setContentLength(gzip.length);
//      out.write(gzip);
//      /** 采用压缩方式，则需注释调这段代码-结束 **/
//      out.flush();
//      out.close();
//    } catch (Exception e) {
//
//    }
//  }
//
//
//  private GridFsResource convertGridFSFile2Resource(GridFSFile gridFsFile) {
//    GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(gridFsFile.getObjectId());
//    return new GridFsResource(gridFsFile, gridFSDownloadStream);
//  }
//}
//
