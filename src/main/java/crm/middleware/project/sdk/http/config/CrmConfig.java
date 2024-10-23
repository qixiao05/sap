package crm.middleware.project.sdk.http.config;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CrmConfig {

    public static String CLIENT_NAME;
    public static String CLIENT_ID;
    public static String CLIENT_SECRET;
    public static String USER_NAME;
    public static String PASSWORD;
    public static String SECURITY;
    public static String REDIRECT_URI;
    public static String API_URL;

    public static String taskOpen;
    public static String notifyOn;
    public static String adminId;

    //自定义查询分页最大数
    public static int limitNum    = 300;
    public static int limitNum_V2 = 100;//官方文档写返回100条

    /**
     * 通知消息MAP，相同的消息同一天只发一次
     */
    public static ConcurrentHashMap<String, Set<String>> notifyMap = new ConcurrentHashMap<String, Set<String>>();

    public static boolean checkNotify(String key) {
        if (CrmConfig.notifyOn.equals("ON")) {
            String datefformat = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            if (notifyMap.size() > 1) {
                cleanMap(datefformat);
            }
            Set<String> set = notifyMap.get(datefformat);
            if (set == null) {
                set = new HashSet<>();
                notifyMap.put(datefformat, set);
            }
            if (set.contains(key)) {
                return false;
            } else {
                set.add(key);
            }
            return true;
        } else {
            return true;
        }
    }

    private static void cleanMap(String datefformat) {
        Set<String> set = notifyMap.get(datefformat);
        notifyMap.clear();
        notifyMap.put(datefformat, set);
    }


}
