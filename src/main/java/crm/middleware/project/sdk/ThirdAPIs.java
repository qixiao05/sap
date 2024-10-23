package crm.middleware.project.sdk;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


import crm.middleware.project.sdk.http.config.CrmApiUrl;
import crm.middleware.project.sdk.http.rest.RestClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Description:
 * @Auther: Shenms
 * @Date: 2019.04.10 10:32
 * @Version: 1.0
 */
@Slf4j
public class ThirdAPIs {

    public static JSONObject get(String url, Map<String, String> params){
        return RestClient.getInstance().get(url, params);
    }

    public static JSONObject post(String url, String params){
        return RestClient.getInstance().post(url, params);
    }

    public static JSONObject post(String url, Map<String, String> params) throws Exception {
        return RestClient.getInstance().post(url, params);
    }

    public static JSONObject patch(String url, String params) throws Exception {
        return RestClient.getInstance().patch(url, params);
    }

    public static JSONObject delete(String url) throws Exception {
        return RestClient.getInstance().delete(url);
    }

    public static JSONObject put(String url, String params) throws Exception {
        return RestClient.getInstance().put(url, params);
    }

    public static JSONObject put(String url) throws Exception {
        return RestClient.getInstance().put(url, null);
    }

    /** 自定义查询 */
    public static JSONArray queryArrayApi(String objectName, String selectColumn, String condition, String orders) {
        StringBuilder sql = new StringBuilder("select ");
        sql.append(selectColumn);
        sql.append(" from ");
        sql.append(objectName);
        if (null != condition && !"".equals(condition)) {
            sql.append(" where ");
            sql.append(condition);
        }

        if (null != orders && !"".equals(orders)) {
            sql.append(" ");
            sql.append(orders);
        } else {
            sql.append(" order by id");
        }

        JSONArray arrays = new JSONArray();
        doQueryByApi(sql.toString(), arrays, 0);
        return arrays;
    }

    private static void doQueryByApi(String sql, JSONArray arrays, int offset) {
        int num = 300;
        Map<String, String> map = new HashMap<String, String>();
        StringBuilder limitBuilder = new StringBuilder(" limit ");
        limitBuilder.append(offset);
        limitBuilder.append(",");
        limitBuilder.append(num);
        map.put("q", sql + limitBuilder.toString());
        try {
            JSONObject result = post(CrmApiUrl.url_v1_query, map);
            log.info("doQueryByApi result: " + result + ";param: " + map.toString());
            if (null == result.get("error_code")) {
                Integer totalSize = result.getInteger("totalSize");
                if (0 < result.getInteger("count")) {
                    arrays.addAll(result.getJSONArray("records"));
                }
                if (num < totalSize - offset) {
                    doQueryByApi(sql, arrays, offset+num);
                }
            }else{
                throw new Exception(result.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage().toString());
        }

    }

    public static  Map<String, String> queryMapApi(Set<String> set, String entity, String selectColumns, String key, String value) {
        int limitNum = 30;
        Map<String, String> map = new HashMap<String, String>();
        log.info("setNo:" + StringUtils.join(set, ","));
        if(StringUtils.join(set, ",").split(",")[0].length() >= 20){
            limitNum = 20;
        }
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<String>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayApi(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size() ; j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json.getString(value));
                        }
                    }
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayApi(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size() ; j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json.getString(value));
                    }
                }
                tempset.clear();
            }
        } else {
            JSONArray arry = queryArrayApi(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size() ; j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
        }
        return map;
    }

    public static  Map<Object, JSONObject> queryMapApi(Set<String> set, String entity, String selectColumns, String key) {
        int limitNum = 30;
        Map<Object, JSONObject> map = new HashMap<>();
        log.info("setNo:" + StringUtils.join(set, ","));
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayApi(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size() ; j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.get(key), json);
                        }
                    }
                    //log.info("setArray:" + arry.toString());
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayApi(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size() ; j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.get(key), json);
                    }
                }
                //log.info("setArray:" + arry.toString());
                tempset.clear();
            }
        } else {
            JSONArray arry = queryArrayApi(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size() ; j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.get(key), json);
                }
            }
        }
        return map;
    }


    //自定义对象创建
    public static JSONObject createCustomize(JSONObject createJson) {
        log.info("createCustomize params: " + createJson.toString());
        JSONObject rsJson = null;
        try {
            rsJson = post(CrmApiUrl.url_v1 + "/customize/create",createJson.toString());
            log.info("createCustomize result: " + rsJson.toString());
        } catch (Exception e) {
            e.printStackTrace();
            if(rsJson == null)
                rsJson = new JSONObject();
            rsJson.put("error_code",30019);
            rsJson.put("message","调用异常");
        }
        return rsJson;
    }

    //自定义对象更新
    public static JSONObject updateCustomize(JSONObject updateJson) {
        log.info("updateCustomize params: " + updateJson.toString());
        JSONObject rsJson = null;
        try {
            rsJson = post(CrmApiUrl.url_v1 + "/customize/update",updateJson.toString());
            log.info("updateCustomize result: " + rsJson.toString());

        } catch (Exception e) {
            e.printStackTrace();
            if(rsJson == null)
                rsJson = new JSONObject();
            rsJson.put("error_code",30019);
            rsJson.put("message","调用异常");
        }
        return rsJson;
    }

    public static JSONObject pushCrmData(String url, JSONObject jsonObject) {
        log.info("pushCrmData params: " + jsonObject.toString());
        JSONObject rsJson = null;
        try {
            rsJson = post(url,jsonObject.toString());
            log.info("pushCrmData result: " + rsJson.toString());
        } catch (Exception e) {
            e.printStackTrace();
            rsJson.put("error_code",30019);
            rsJson.put("message","调用异常");
        }
        return rsJson;
    }

}
