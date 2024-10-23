package crm.middleware.project.util;


import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Properties;

@Slf4j
public class PropertiesOperationUtil {

    /**
     * 根据key和文件名取得value值
     *
     * @param key
     * @return
     */
    public static String getValue(String fileName,String key) {
        Properties pps = new Properties();
        try {
            String filePath = PropertiesOperationUtil.class.getClassLoader().getResource(fileName).getPath();
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            pps.load(inputStream);
            String value = pps.getProperty(key);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return fileName+"路径找不到";
        }
    }

    public static void setValue(String fileName,String key,String value){
        /*try {
            InputStream inputStream = PropertiesOperationUtil.class.getClassLoader().getResourceAsStream(fileName);
            String filePath = PropertiesOperationUtil.class.getClassLoader().getResource(fileName).getPath();
            OutputStream out = new FileOutputStream(filePath);
            Properties pro = new Properties();

            pro.load(inputStream);
            pro.setProperty(key, value);
            //以适合使用 load 方法加载到 Properties 表中的格式，
            //将此 Properties 表中的属性列表（键和元素对）写入输出流
            pro.store(out, "Update " + key + " name");
        } catch (IOException e) {
            log.info(e.getMessage());
        }*/

        Properties pps = new Properties();
        InputStream in;
        try {
            String filePath = PropertiesOperationUtil.class.getClassLoader().getResource(fileName).getPath();
            in = new FileInputStream(filePath);
            //从输入流中读取属性列表（键和元素对）
            pps.load(in);
            //调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
            //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
            OutputStream out = new FileOutputStream(filePath);
            pps.setProperty(key, value);
            //以适合使用 load 方法加载到 Properties 表中的格式，
            //将此 Properties 表中的属性列表（键和元素对）写入输出流
            pps.store(out, "Update " + key + " name");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        String a = getValue("updateTime.properties","jms_update_client_time");
        System.out.println("前value:"+a);
        setValue("updateTime.properties","jms_update_client_time", DateUtil8.getNowTime_EN());
        String b = getValue("updateTime.properties","jms_update_client_time");
        System.out.println("后value:"+b);

    }

}
