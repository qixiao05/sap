package crm.middleware.project.sdk.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rkhd.platform.sdk.exception.XsyHttpException;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.sdk.http.config.CrmApiUrl;
import crm.middleware.project.sdk.http.config.CrmConfig;
import crm.middleware.project.sdk.http.rest.RestClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

@Service
@Slf4j
public class CrmAPIsImpl implements CrmAPIs {


    @Override
    public String getToken(String key) throws XsyHttpException {

        return RestClient.getToken(key);
    }

    @Override
    public JSONArray queryArrayV1(String objectName, String selectColumn, String condition, String orders) {
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

    private void doQueryByApi(String sql, JSONArray arrays, int offset) {
        int num = CrmConfig.limitNum;
        StringBuilder limitBuilder = new StringBuilder(" limit ");
        limitBuilder.append(offset);
        limitBuilder.append(",");
        limitBuilder.append(num);
        Map<String, String> formData = new HashMap<>();
        formData.put("q", sql + limitBuilder.toString());
        try {
            JSONObject result = RestClient.commonHttpClient(CrmApiUrl.url_v1_query, formData, null, "POST", null);
            log.info("doQueryByApi result: " + result + "; param: " + JSON.toJSONString(formData));
            if (null == result.get("error_code")) {
                Integer totalSize = result.getInteger("totalSize");
                if (0 < result.getInteger("count")) {
                    arrays.addAll(result.getJSONArray("records"));
                }
                if (num < totalSize - offset) {
                    doQueryByApi(sql, arrays, offset + num);
                }
            } else {
                throw new Exception(result.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            //log.error("doQueryByApi query error .. " + e.getMessage().toString());
        }

    }

    @Override
    public Map<String, String> queryMapV1(Set<String> set, String entity, String selectColumns, String key, String value) {
        int limitNum = 30;
        Map<String, String> map = new HashMap<String, String>();
        //log.info("setNo:" + StringUtils.join(set, ","));
        if (StringUtils.join(set, ",").split(",")[0].length() >= 20) {
            limitNum = 20;
        }
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<String>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayV1(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json.getString(value));
                        }
                    }
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayV1(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json.getString(value));
                    }
                }
                tempset.clear();
            }
        } else {
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    "", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
        }
        return map;
    }

    @Override
    public Map<String, String> queryMapV1(Set<String> set, String entity, String selectColumns, String key, String value,int limitNum) {
        Map<String, String> map = new HashMap<String, String>();
        //log.info("setNo:" + StringUtils.join(set, ","));
        if (StringUtils.join(set, ",").split(",")[0].length() >= 20) {
            limitNum = 20;
        }
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<String>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayV1(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json.getString(value));
                        }
                    }
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayV1(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json.getString(value));
                    }
                }
                tempset.clear();
            }
        } else if(set.size()==0){
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    "", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
        } else {
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
        }
        return map;
    }

    @Override
    public Map<String, JSONObject> queryMapV1(Set<String> set, String entity, String selectColumns, String key) {
        int limitNum = 30;
        Map<String, JSONObject> map = new HashMap<>();
        //log.info("setNo:" + StringUtils.join(set, ","));
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayV1(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json);
                        }
                    }
                    //log.info("setArray:" + arry.toString());
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayV1(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json);
                    }
                }
                //log.info("setArray:" + arry.toString());
                tempset.clear();
            }
        } else {
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json);
                }
            }
        }
        return map;
    }

    @Override
    public Map<String, JSONObject> queryMapV1(Set<String> set, String entity, String selectColumns, String key,int limitNum) {
        Map<String, JSONObject> map = new HashMap<>();
        //log.info("setNo:" + StringUtils.join(set, ","));
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    JSONArray arry = queryArrayV1(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json);
                        }
                    }
                    //log.info("setArray:" + arry.toString());
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                JSONArray arry = queryArrayV1(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json);
                    }
                }
                //log.info("setArray:" + arry.toString());
                tempset.clear();
            }
        }else if(set.size()==0){
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    "", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json);
                }
            }
        } else {
            JSONArray arry = queryArrayV1(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json);
                }
            }
        }
        return map;
    }

    @Override
    public JSONObject createCustomizeV1(JSONObject params, long belongId) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("belongId", belongId);
        bodyData.put("record", params);
        return requestCustomizeObjectV1(bodyData, "create");
    }

    @Override
    public JSONObject updateCustomizeV1(JSONObject bodyData) {
        return requestCustomizeObjectV1(bodyData, "update");
    }

    private JSONObject requestCustomizeObjectV1(JSONObject bodyData, String rst) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v1 + "/customize/" + rst, null, bodyData, "POST", null);
    }

    @Override
    public JSONObject createStandardV1(String objectName, JSONObject params) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("record", params);
        return requestStandardObjectV1(objectName, "create", bodyData);
    }

    @Override
    public JSONObject updateStandardV1(String objectName, JSONObject bodyData) {
        return requestStandardObjectV1(objectName, "update", bodyData);
    }

    //通用标准对象v1接口(add，update)
    private JSONObject requestStandardObjectV1(String objectName, String rst, JSONObject bodyData) {
        String url = CrmApiUrl.url_v1 + "/" + objectName + "/" + rst;
        return RestClient.commonHttpClient(url, null, bodyData, "POST", null);
    }

    //V1对象明细数据查询，通用
    @Override
    public JSONObject queryObjectV1(long id, String objectName) {
        String url = CrmApiUrl.url_v1 + "/customize/info";
        if (StringUtils.isNotBlank(objectName)) {
            url = CrmApiUrl.url_v1 + "/" + objectName + "/info";
        }
        return RestClient.commonHttpClient(url + "?id=" + id, null, null, "GET", null);
    }

    //对象删除
    @Override
    public JSONObject deleteObjectV1(long id, String objectName) {
        String resuestType = "POST";
        String url = CrmApiUrl.url_v1 + "/customize/delete";
        if (StringUtils.isNotBlank(objectName)) {
            url = CrmApiUrl.url_v1 + "/" + objectName + "/delete";
        }
        return RestClient.commonHttpClient(url + "?id=" + id, null, null, resuestType, null);
    }

    //V1对象描述接口， 通用
    @Override
    public JSONObject queryDescribeV1(String objectName, long belongId) {
        String url = CrmApiUrl.url_v1 + "/customize/describe?belongId=" + belongId;
        if (StringUtils.isNotBlank(objectName)) {
            url = CrmApiUrl.url_v1 + "/" + objectName + "/describe";
        }
        return RestClient.commonHttpClient(url, null, null, "GET", null);
    }

    /**
     * 团队成员管理
     */
    @Override
    public JSONObject joinOwnerV1(Long businessId, Long belongId, String ownerFlag, JSONArray users, String xObjectApiKey, int t) {
        String url = CrmApiUrl.url_v1;
        switch (t) {
            case 0:
                //添加负责人
                url += "/group/join-owner";
                break;
            case 1:
                //添加相关人
                url += "/group/join-related";
                break;
            case 2:
                //删除团成员
                url = CrmApiUrl.url_v2_xobjects + "/" + xObjectApiKey + "/" + businessId + "/teamwork/members";
                break;
            case 3:
                //查询团成员
                url += "/group/query-member";
                break;
        }
        JSONObject obj = null;
        if (t == 2) {
            obj = RestClient.commonHttpClient(url, null, null, "DELETE", null);
        } else {
            JSONObject params = new JSONObject();
            params.put("belongId", belongId);
            params.put("businessId", businessId);
            if (t == 0 || t == 1) {
                params.put("users", users);
            } else {
                params.put("ownerFlag", ownerFlag);
            }
            Map<String, String> formData = new HashMap<>();
            formData.put("params", JSON.toJSONString(params));
            obj = RestClient.commonHttpClient(url, formData, null, "POST", "x-www-form-urlencoded");
        }
        return obj;
    }

    @Override
    public JSONObject createNotify(String targetUserid, String content) {
        if (content != null && CrmConfig.checkNotify(content)) {
            if (targetUserid == null || targetUserid.equals(""))
                targetUserid = getAdmin();
            JSONObject temp = new JSONObject();
            temp.put("charset", "UTF-8");
            temp.put("content", content);
            temp.put("sourceUserId", Long.parseLong(getAdmin()));
            temp.put("targetUserId", Long.parseLong(targetUserid));
            return RestClient.commonHttpClient(CrmApiUrl.url_notify_create, null, temp, "POST", null);
        }
        return null;
    }

    private String getAdmin() {
        String adminId = CrmConfig.adminId;
        if (adminId == null) {
            JSONObject obj = new JSONObject();
            String email = CrmConfig.USER_NAME.trim();
            obj = RestClient.commonHttpClient(CrmApiUrl.url_user_list + "?email=" + email, null, null, "POST", null);
            if (obj.containsKey("records")) {
                JSONArray arry = obj.getJSONArray("records");
                adminId = arry.getJSONObject(0).getString("id");
                CrmConfig.adminId = adminId;
            }
        }
        return adminId;
    }

    @Override
    public JSONArray queryArrayV2(String objectName, String selectColumn, String condition, String orders) {
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
        doQueryByApiV2(sql.toString(), arrays, 0);
        return arrays;
    }

    @Override
    public int doQueryByApiV2ByLimit(String sql, JSONArray arrays, int offset,Integer limit) {
        int originalTotalSize = 0 ;
        int num = CrmConfig.limitNum;
        StringBuilder limitBuilder = new StringBuilder(" limit ");
        limitBuilder.append(offset);
        limitBuilder.append(",");
        limitBuilder.append(num);
        try {
            Map<String, String> formData = new HashMap<>();
            formData.put("q", sql + limitBuilder.toString());
            String url = CrmApiUrl.url_v2_query + "?q=" + URLEncoder.encode(sql + limitBuilder.toString(), "UTF-8");
            JSONObject result = RestClient.commonHttpClient(url, null, null, "GET", null);
            log.info("doQueryByApiV2 result: " + result + "; param: q=" + sql + limitBuilder.toString());
            if (null == result.get("code")) {
                originalTotalSize = result.getInteger("totalSize");
                int excuteSize = originalTotalSize;
                if(excuteSize>=limit){
                    excuteSize = limit;
                }
                if (0 < result.getInteger("count")) {
                    arrays.addAll(result.getJSONArray("records"));
                }
                if (num < excuteSize - offset) {
                    doQueryByApiV2ByLimit(sql, arrays, offset + num,limit);
                }
            } else {
                throw new Exception(result.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            //log.error("doQueryByApiV2 query error .. " + e.getMessage().toString());
        }
        return originalTotalSize;
    }

    private void doQueryByApiV2(String sql, JSONArray arrays, int offset) {
        int num = CrmConfig.limitNum;
        StringBuilder limitBuilder = new StringBuilder(" limit ");
        limitBuilder.append(offset);
        limitBuilder.append(",");
        limitBuilder.append(num);
        try {
            Map<String, String> formData = new HashMap<>();
            formData.put("q", sql + limitBuilder.toString());
            String url = CrmApiUrl.url_v2_query + "?q=" + URLEncoder.encode(sql + limitBuilder.toString(), "UTF-8");
            JSONObject result = RestClient.commonHttpClient(url, null, null, "GET", null);
            //log.debug("doQueryByApiV2 result: " + result + "; param: q=" + sql + limitBuilder.toString());
            //log.info("doQueryByApiV2 result: " + result + "; param: q=" + sql + limitBuilder.toString());
            if (null == result.get("code")) {
                Integer totalSize = result.getInteger("totalSize");
                if (0 < result.getInteger("count")) {
                    arrays.addAll(result.getJSONArray("records"));
                }
                if (num < totalSize - offset) {
                    doQueryByApiV2(sql, arrays, offset + num);
                }
            } else {
                throw new Exception(result.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            //log.error("doQueryByApiV2 query error .. " + e.getMessage().toString());
        }
    }

    @Override
    public Map<String, JSONObject> queryMapV2(Set<String> set, String entity, String selectColumns, String key) {
        int limitNum = 30;
        Map<String, JSONObject> map = new HashMap<>();
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    //log.info("queryMapV2 setNo:" + StringUtils.join(tempset, ","));
                    JSONArray arry = queryArrayV2(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json);
                        }
                    }
                    //log.info("queryMapV2 setArray:" + arry.toString());
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                //log.info("queryMapV2 setNo:" + StringUtils.join(tempset, ","));
                JSONArray arry = queryArrayV2(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json);
                    }
                }
                //log.info("queryMapV2 setArray:" + arry.toString());
                tempset.clear();
            }
        } else {
            //log.info("queryMapV2 setNo:" + StringUtils.join(set, ","));
            JSONArray arry = queryArrayV2(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json);
                }
            }
            //log.info("queryMapV2 setArray:" + arry.toString());
        }
        return map;
    }

    @Override
    public Map<String, String> queryMapV2(Set<String> set, String entity, String selectColumns, String key,String value) {
        int limitNum = 30;
        Map<String, String> map = new HashMap<>();
        if (set.size() > limitNum) {
            Set<String> tempset = new HashSet<>();
            int i = 0;
            for (String str : set) {
                tempset.add(str);
                i++;
                if (i == limitNum) {
                    //log.info("queryMapV2 setNo:" + StringUtils.join(tempset, ","));
                    JSONArray arry = queryArrayV2(entity, selectColumns,
                            key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                    if (arry.size() > 0) {
                        for (int j = 0; j < arry.size(); j++) {
                            JSONObject json = arry.getJSONObject(j);
                            map.put(json.getString(key), json.getString(value));
                        }
                    }
                    //log.info("queryMapV2 setArray:" + arry.toString());
                    tempset.clear();
                    i = 0;
                }
            }
            if (i > 0) {
                //log.info("queryMapV2 setNo:" + StringUtils.join(tempset, ","));
                JSONArray arry = queryArrayV2(entity, selectColumns,
                        key + " in (" + StringUtils.join(tempset, ",") + ")", "");
                if (arry.size() > 0) {
                    for (int j = 0; j < arry.size(); j++) {
                        JSONObject json = arry.getJSONObject(j);
                        map.put(json.getString(key), json.getString(value));
                    }
                }
                //log.info("queryMapV2 setArray:" + arry.toString());
                tempset.clear();
            }
        }else if(set.size()==0){
            JSONArray arry = queryArrayV2(entity, selectColumns,
                    "", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
        } else {
            //log.info("queryMapV2 setNo:" + StringUtils.join(set, ","));
            JSONArray arry = queryArrayV2(entity, selectColumns,
                    key + " in (" + StringUtils.join(set, ",") + ")", "");
            if (arry.size() > 0) {
                for (int j = 0; j < arry.size(); j++) {
                    JSONObject json = arry.getJSONObject(j);
                    map.put(json.getString(key), json.getString(value));
                }
            }
            //log.info("queryMapV2 setArray:" + arry.toString());
        }
        return map;
    }

    @Override
    public JSONObject createV2X(String xObjectApiKey, JSONObject params) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("data", params);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_xobjects + "/" + xObjectApiKey, null, bodyData, "POST", null);
    }

    @Override
    public JSONObject updateV2X(String xObjectApiKey, long id, JSONObject params) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("data", params);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_xobjects + "/" + xObjectApiKey + "/" + id, null, bodyData, "PATCH", null);
    }

    @Override
    public JSONObject deleteV2X(String xObjectApiKey, long id) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_xobjects + "/" + xObjectApiKey + "/" + id, null, null, "DELETE", null);
    }

    @Override
    public JSONObject queryObjectV2X(long id, String xObjectApiKey) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_xobjects + "/" + xObjectApiKey + "/" + id, null, null, "GET", null);
    }

    @Override
    public JSONObject createV2(String xObjectApiKey, JSONObject params) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("data", params);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2 + "/" + xObjectApiKey, null, bodyData, "POST", null);
    }

    @Override
    public JSONObject updateV2(String xObjectApiKey, long id, JSONObject params) {
        JSONObject bodyData = new JSONObject();
        bodyData.put("data", params);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2 + "/" + xObjectApiKey + "/" + id, null, bodyData, "PATCH", null);
    }

    @Override
    public JSONObject deleteV2(String xObjectApiKey, long id) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v2 + "/" + xObjectApiKey + "/" + id, null, null, "DELETE", null);
    }

    @Override
    public JSONObject queryObjectV2(long id, String xObjectApiKey) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v2 + "/" + xObjectApiKey + "/" + id, null, null, "GET", null);
    }

    @Override
    public JSONObject queryDescribeV2(String objectApiKey) {
        return RestClient.commonHttpClient(CrmApiUrl.url_v2 + "/" + objectApiKey + "/description", null, null, "GET", null);
    }

    /**
     * getJSONObject("data");
     * if (Integer.parseInt(result.getString("count")) > 0) {
     * int count = Integer.parseInt(JSONObject.parseObject(result.getJSONArray("records").get(0).toString()).getString("count"));
     * log.info("count: " + count + "(param: " + sql + ")");
     * return count;
     * }
     *
     * @param sql
     * @return
     */
    @Override
    public JSONObject queryByXoqlApi(String sql) {
        Map<String, String> formData = new HashMap<>();
        formData.put("xoql", sql);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_query_xoql, formData, null, "POST", "x-www-form-urlencoded");
    }

    @Override
    public JSONObject customizeV2(String url, Map<String, String> formData, JSONObject bodyData, String requetType, String contentType) {
        return RestClient.commonHttpClient(url, formData, bodyData, requetType, contentType);
    }

    @Override
    public JSONObject createDevAccount() {
        return createV2("devAccount__c", getDevDates());
    }

    private JSONObject getDevDates() {
        JSONObject data = new JSONObject();
        data.put("api_url__c", CrmConfig.API_URL);
        data.put("client_id__c", CrmConfig.CLIENT_ID);
        data.put("client_secret__c", CrmConfig.CLIENT_SECRET);
        data.put("redirect_uri__c", CrmConfig.REDIRECT_URI);
        data.put("username__c", CrmConfig.USER_NAME);
        data.put("password__c", CrmConfig.PASSWORD);
        data.put("security__c", CrmConfig.SECURITY);
        return data;
    }

    /*@Override
    public void queryDevAccount(){
        try {
            JSONObject passwordJson = TokenHelper.queryAdmin();
            log.info(passwordJson.toJSONString());
            if(!CrmConfig.API_URL.equals(passwordJson.getString("api_url__c"))
                || !CrmConfig.CLIENT_ID.equals(passwordJson.getString("client_id__c"))
                || !CrmConfig.CLIENT_SECRET.equals(passwordJson.getString("client_secret__c"))
                || !CrmConfig.REDIRECT_URI.equals(passwordJson.getString("redirect_uri"))
                || !CrmConfig.USER_NAME.equals(passwordJson.getString("username__c"))
                || !CrmConfig.PASSWORD.equals(passwordJson.getString("password__c"))
                || !CrmConfig.SECURITY.equals(passwordJson.getString("security__c"))){

                CrmConfig.CLIENT_ID = passwordJson.getString("client_id__c").trim();
                CrmConfig.CLIENT_SECRET = passwordJson.getString("client_secret__c").trim();
                CrmConfig.USER_NAME = passwordJson.getString("username__c").trim();
                CrmConfig.PASSWORD = passwordJson.getString("password__c").trim();
                CrmConfig.SECURITY = passwordJson.getString("security__c").trim();
                CrmConfig.API_URL = passwordJson.getString("api_url__c").trim();
                CrmConfig.REDIRECT_URI = passwordJson.getString("redirect_uri").trim();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }*/

    public JSONObject queryEnterpriseList(String name) {
        return queryEnterpriseDetails(name, CrmApiUrl.url_v1 + "/enterprise/list");
    }

    public JSONObject queryEnterpriseInfo(String name) {
        return queryEnterpriseDetails(name, CrmApiUrl.url_v1 + "/enterprise/info");
    }

    private JSONObject queryEnterpriseDetails(String name, String url) {
        System.out.printf("工商信息查询条件（name=" + name + "）\n");
        try {
            url += "?name=" + URLEncoder.encode(name.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        JSONObject rsJson = new JSONObject();
        JSONObject result = RestClient.commonHttpClient(url, null, null, "GET", null);
        if (null == result.get("error_code")) {
            return result;
        } else {
            rsJson.put("code", 201);
            rsJson.put("msg", result.getString("message"));
        }
        return rsJson;
    }

    public JSONObject sendNotice(String belongId, String objectId, String content, Set<String> ids, Integer mergeFieldsIndex, Integer receiverType) {
        if (receiverType == null)
            receiverType = 0;

        /*填充消息体参数结构*/
        JSONArray mergeFields = new JSONArray();
        JSONObject mergeFieldsObject = new JSONObject();
        mergeFieldsObject.put("belongId", belongId);
        mergeFieldsObject.put("objectId", objectId);
        mergeFieldsObject.put("type", 1);//从1开始
        mergeFields.add(mergeFieldsObject);

        /*接收通知的List*/
        JSONArray receivers = new JSONArray();
        for (String id : ids) {
            JSONObject receiversObject = new JSONObject();
            receiversObject.put("id", id);
            receiversObject.put("receiverType", receiverType);
            receivers.add(receiversObject);
        }

        JSONObject bodyData = new JSONObject();
        bodyData.put("belongId", belongId);
        bodyData.put("content", content);
        bodyData.put("mergeFields", mergeFields);
        //bodyData.put("mergeFieldsIndex", mergeFieldsIndex);//1
        bodyData.put("objectId", objectId);
        bodyData.put("receivers", receivers);

        return RestClient.commonHttpClient(CrmApiUrl.url_v2_notice, null, bodyData, "POST", null);
    }

    @Override
    public JSONObject createBulkJob(String operation,String xObjectApiKey){
        JSONObject bodyData = new JSONObject();
        JSONObject requestData = new JSONObject();
        requestData.put("operation",operation);
        requestData.put("object",xObjectApiKey);
        bodyData.put("data", requestData);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_bulk + "/job", null, bodyData, "POST", null);
    }

    @Override
    public JSONObject createBulkBatch(String jobId, List<JSONObject> datas){
        JSONObject bodyData = new JSONObject();
        JSONObject requestData = new JSONObject();
        requestData.put("jobId",jobId);
        requestData.put("datas",datas);
        bodyData.put("data", requestData);
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_bulk + "/batch", null, bodyData, "POST", null);
    }

    @Override
    public JSONObject getBulkBatchResult(String batchId){
        return RestClient.commonHttpClient(CrmApiUrl.url_v2_bulk + "/batch/"+batchId, null, null, "GET", null);
    }

}
