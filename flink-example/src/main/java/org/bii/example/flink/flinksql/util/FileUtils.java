package org.bii.example.flink.flinksql.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName FileUtils
 * @Description TODO
 * @Author heshaozhong
 * @Date 5:50 PM 2020/1/7
 */
@Slf4j
public class FileUtils {

    /**
     * 向指定目录文件写入数据
     * @param content
     * @param path
     * @param isApend
     */
    public static void saveToFile(String content, String path, Boolean isApend) {
        byte[] sourceByte = content.getBytes();
        if(null != sourceByte){
            try {
                File file = new File(path);		//文件路径（路径+文件名）
                if (!file.exists()) {	//文件不存在则创建文件，先创建目录
                    File dir = new File(file.getParent());
                    dir.mkdirs();
                    file.createNewFile();
                }
                FileOutputStream outStream = new FileOutputStream(file,isApend);	//文件输出流用于将数据写入文件
                outStream.write(sourceByte);
                outStream.close();	//关闭文件输出流
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 逐行读取内容并存储到List中
     * @param path
     * @return
     */
    public static List<String> readFile(String path) {
        List<String> results = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                results.addAll(Arrays.asList(lineTxt.split(",")));
            }
            br.close();
        } catch (Exception e) {
            log.error("read file {} errors :",path);
        }

        return results;
    }
    
    /**
     * 逐行读取内容并存储到List中
     * @param path
     * @return
     */
    public static String file2String(String path) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String lineTxt;
            while ((lineTxt = br.readLine()) != null) {
                sb.append(lineTxt);
            }
            br.close();
        } catch (Exception e) {
            log.error("read file {} errors :",path);
        }
        
        return sb.toString();
    }
    
    
    /**
     * 逐行读取内容并存储到List中
     * @param path
     * @return
     */
    public static List<String> readLineFromFile(String path) {
        List<String> res = new ArrayList();
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String lineTxt;
            while ((lineTxt = br.readLine()) != null) {
                res.add(new String(lineTxt));
            }
            br.close();
        } catch (Exception e) {
            log.error("read file {} errors :",path);
        }
        
        return res;
    }
    
}
