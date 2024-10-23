package crm.middleware.project.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.rkhd.platform.sdk.http.RkhdHttpClient;
import com.rkhd.platform.sdk.http.RkhdHttpData;
import com.rkhd.platform.sdk.http.handler.ResponseBodyHandlers;
import com.rkhd.platform.sdk.log.Logger;
import com.rkhd.platform.sdk.log.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.net.URLEncoder;
import java.util.*;



/**
 * 仁科互动(RkhdHttpClient)公用方法
 *
 * @author Administrator
 */
public class RkhdHelper {

    private static final Logger logger = LoggerFactory.getLogger();

    private final static int V2_LENGTH = 300;

    private final static int XOQL_LENGTH = 3000;
    /**
     * totalSize字符串
     */
    private static final String TOTAL_SIZE_STR = "totalSize";
    /**
     * code字符串
     */
    public static final String CODE_STR = "code";
    /**
     * V2接口成功代码
     */
    public static final Integer RESULT_CODE = 200;
    /**
     * V1接口成功代码
     */
    private static final Integer STATUS_CODE = 0;
    /**
     * 日志显示的最大长度
     */
    private static final int LOG_MAX_LENGTH = 2900;
    /**
     * query最大sql长度
     */
    private final static int QUERY_LENGTH = 1000;
    /**
     * query预留长度
     */
    private final static int QUERY_OBL_LENGTH = 50;
    /**
     * 用户访问频率超出限制
     */
    private final static Integer USER_FREQUENCY_LIMIT_ERROR = 1020025;

    /**
     * 打印日志长度超过3000的方案
     *
     * @param log 日志文本
     */
    public static void log(String log) {

        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        int lineNumber = Thread.currentThread().getStackTrace()[2].getLineNumber();
        log = className + " | " + methodName + " | " + lineNumber + " : " + log;
        if (StringUtils.isNotBlank(log)) {
            int size = log.length() / LOG_MAX_LENGTH;
            for (int index = 0; index <= size; index++) {
                String childStr;
                if (index == size) {
                    childStr = log.substring(index * LOG_MAX_LENGTH);
                } else {
                    childStr = log.substring(index * LOG_MAX_LENGTH, (index + 1) * LOG_MAX_LENGTH);
                }
                if (size == 0) {
                    logger.info(childStr);
                } else {
                    logger.info("Part " + (index + 1) + " | " + childStr);
                }
            }
        }
    }

    /**
     * 用户访问频率超出限制
     * 后休眠
     */
    public static void threadSleep() {
        try {
            int max = 1200;
            int min = 600;
            Random random = new Random();
            int s = random.nextInt(max - min) + min + 1;
            Thread.sleep(s);
        } catch (Exception e) {
            logger.error("threadSleep:" + e.getMessage(), e);
        }
    }

    /**
     * v2查询
     *
     * @param client RkhdHttpClient类型的连接器-null时自动生成
     * @param sql    sql
     * @return JSONArray
     */
    public static JSONArray v2Query(RkhdHttpClient client, String sql) {
        JSONArray records = new JSONArray();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            String baseUrl = "/rest/data/v2/query?q=";
            boolean hasData;
            int start = 0;
            do {
                hasData = false;
                String url = baseUrl + URLEncoder.encode(sql + " limit " + start + " , " + V2_LENGTH, "utf-8");
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("GET")
                        .callString(url)
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  sql:" + sql);
                if (response != null && response.containsKey(CODE_STR)) {
                    if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                        JSONObject resultJsonObject = response.getJSONObject("result");
                        int count = resultJsonObject.getIntValue("count");
                        int totalSize = resultJsonObject.getIntValue(TOTAL_SIZE_STR);
                        if (count > 0) {
                            records.addAll(response.getJSONObject("result").getJSONArray("records"));
                        }
                        start = start + V2_LENGTH;
                        if (totalSize > start) {
                            hasData = true;
                        }
                    } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                        threadSleep();
                        return v2Query(client, sql);
                    } else {
                        logger.error("v2Query 未查询到结果：" + JSONObject.toJSONString(response) + "sql:" + sql);
                    }
                } else {
                    logger.error("v2Query 未查询到结果：" + JSONObject.toJSONString(response) + "sql:" + sql);
                }
            } while (hasData);
        } catch (Exception e) {
            logger.error("v2Query 报错：" + e.getMessage(), e);
        }
        return records;
    }

    /***
     *v2查询 拼接 where in
     * @param client RkhdHttpClient类型的连接器-null时自动生成
     * @param baseSql  select id from  where account in %s
     * @param whereList List<String>
     * @return JSONArray
     */
    public static JSONArray v2QuerySplitIn(RkhdHttpClient client, String baseSql, List<String> whereList) {
        JSONArray records = new JSONArray();
        int sqlLength = QUERY_LENGTH - QUERY_OBL_LENGTH - baseSql.length();
        String tempSql = "";
        for (String item : whereList) {
            if ((tempSql.length() + item.length() + 1) > sqlLength) {
                String sql = String.format(baseSql, "(" + tempSql + ") ");
                logger.debug("formatSql:" + sql);
                records.addAll(v2Query(client, sql));
                tempSql = "";
            }
            if ("".equals(tempSql)) {
                tempSql = item;
            } else {
                tempSql = tempSql + "," + item;
            }
        }
        if (!"".equals(tempSql)) {
            String sql = String.format(baseSql, "(" + tempSql + ") ");
            logger.debug("formatSql:" + sql);
            records.addAll(v2Query(client, sql));
        }
        return records;
    }


    /**
     * 获取xoql结果
     *
     * @param client RkhdHttpClient类型的连接器-null时自动生成
     * @param sql    sql
     * @return JSONArray
     */
    public static JSONArray xoql(RkhdHttpClient client, String sql) {
        JSONArray records = new JSONArray();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            boolean hasData = false;
            int start = 0;
            do {
                String tempSql = sql + " limit " + start + " , " + XOQL_LENGTH;
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("POST")
                        .callString("/rest/data/v2.0/query/xoql")
                        .formData("xoql", tempSql)
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  sql:" + sql);
                if (response != null && response.containsKey(CODE_STR)) {
                    if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                        JSONObject dataJsonObject = response.getJSONObject("data");
                        int count = dataJsonObject.getIntValue("count");
                        if (count == XOQL_LENGTH) {
                            records.addAll(response.getJSONObject("data").getJSONArray("records"));
                            hasData = true;
                        } else {
                            records.addAll(response.getJSONObject("data").getJSONArray("records"));
                            hasData = false;
                        }
                        start = start + XOQL_LENGTH;
                    } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                        threadSleep();
                        return xoql(client, sql);
                    } else {
                        logger.error("xoql 未查询到结果：" + JSONObject.toJSONString(response) + "sql:" + sql);
                    }
                } else {
                    logger.error("xoql 未查询到结果：" + JSONObject.toJSONString(response) + "sql:" + sql);
                }
            } while (hasData);
        } catch (Exception e) {
            logger.error("xoql 报错：" + e.getMessage(), e);
        }
        return records;
    }


    /**
     * 获取实体信息
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名称
     * @param id         实体记录Id
     * @return JSONObject 实体信息
     */
    public static JSONObject getEntityInfoById(RkhdHttpClient client, String entityName, Long id) {
        JSONObject entity = null;
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/" + id)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回结果：" + JSONObject.toJSONString(response) + " entityName:" + entityName + " id:" + id);
            Integer responseCode = response.getIntValue("code");
            if (RESULT_CODE.equals(responseCode)) {
                entity = response.getJSONObject("data");
            } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                threadSleep();
                return getEntityInfoById(client, entityName, id);
            } else {
                logger.error("getEntityInfoById 未查询到结果：" + JSONObject.toJSONString(response) + " entityName:" + entityName + " id:" + id);
            }
        } catch (Exception e) {
            logger.error(" 报错信息：" + e);
        }
        return entity;
    }

    public static JSONObject queryEntityFields(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/description")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  entityName:" + entityName);
            if (response != null && response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONObject dataObject = response.getJSONObject("data");
                    JSONArray fields = dataObject.getJSONArray("fields");
                    for (int i = 0; i < fields.size(); i++) {
                        JSONObject fieldItem = fields.getJSONObject(i);
                        JSONObject option = new JSONObject(true);
                        option.put("apiKey", fieldItem.getString("apiKey"));
                        option.put("label", fieldItem.getString("label"));
                        option.put("type", fieldItem.getString("type"));
                        option.put("itemType", fieldItem.getString("itemType"));
                        option.put("referTo", "");
                        option.put("joinTo", "");
                        //关联字段
                        if (fieldItem.getJSONObject("referTo") != null) {
                            option.put("referTo", fieldItem.getJSONObject("referTo").getString("apiKey"));
                        }
                        //引用字段
                        if (fieldItem.getJSONObject("joinTo") != null) {
                            option.put("joinTo", fieldItem.getJSONObject("joinTo").getString("objectApiKey") + "." + fieldItem.getJSONObject("joinTo").getString("itemApiKey"));
                        }
                        record.put(option.getString(mainKey), option);
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return queryEntityFields(client, entityName, mainKey);
                } else {
                    logger.error("queryItemOptionValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
                }
            }
        } catch (Exception e) {
            logger.error("queryEntityFields 报错信息：" + e.getMessage(), e);
        }
        return record;
    }


    public static JSONObject queryEntityFieldsAndItemOptionValue(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject record = new JSONObject();
        JSONObject crmField = new JSONObject();
        JSONObject valueOption = new JSONObject();
        JSONObject apiKeyOption = new JSONObject();
        JSONObject labelOption = new JSONObject();

        record.put("crmField", crmField);
        record.put("valueOption", valueOption);
        record.put("apiKeyOption", apiKeyOption);
        record.put("labelOption", labelOption);

        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/description")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  entityName:" + entityName);
            if (response != null && response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONObject dataObject = response.getJSONObject("data");
                    JSONArray fields = dataObject.getJSONArray("fields");
                    for (int i = 0; i < fields.size(); i++) {
                        JSONObject fieldItem = fields.getJSONObject(i);
                        JSONObject option = new JSONObject(true);
                        option.put("apiKey", fieldItem.getString("apiKey"));
                        option.put("label", fieldItem.getString("label"));
                        option.put("type", fieldItem.getString("type"));
                        option.put("itemType", fieldItem.getString("itemType"));
                        option.put("referTo", "");
                        option.put("joinTo", "");
                        //关联字段
                        if (fieldItem.getJSONObject("referTo") != null) {
                            option.put("referTo", fieldItem.getJSONObject("referTo").getString("apiKey"));
                        }
                        //引用字段
                        if (fieldItem.getJSONObject("joinTo") != null) {
                            option.put("joinTo", fieldItem.getJSONObject("joinTo").getString("objectApiKey") + "." + fieldItem.getJSONObject("joinTo").getString("itemApiKey"));
                        }
                        crmField.put(option.getString(mainKey), option);

                        JSONObject optionItem1 = new JSONObject();
                        JSONObject optionItem2 = new JSONObject();
                        JSONObject optionItem3 = new JSONObject();
                        String apiKey = fieldItem.getString("apiKey");
                        if ("picklist".equals(fieldItem.getString("type"))) {
                            JSONArray selectItem = fieldItem.getJSONArray("selectitem");
                            if (selectItem.size() > 0) {
                                valueOption.put(apiKey, optionItem1);
                                apiKeyOption.put(apiKey, optionItem2);
                                labelOption.put(apiKey, optionItem3);

                                for (Object itemObject : selectItem) {
                                    JSONObject item = (JSONObject) itemObject;
                                    JSONObject select = new JSONObject();
                                    select.put("label", item.getString("label"));
                                    select.put("value", item.getString("value"));
                                    select.put("apiKey", item.getString("apiKey"));
                                    JSONArray dependentValue = item.getJSONArray("dependentValue");
                                    if (dependentValue != null && dependentValue.size() > 0) {
                                        JSONObject vObject = new JSONObject();
                                        for (Object v : dependentValue) {
                                            vObject.put(v.toString(), v.toString());
                                        }
                                        String vStr = JSONObject.toJSONString(vObject.keySet());
                                        select.put("dependentValue", JSONArray.parseArray(vStr));
                                    }
                                    optionItem1.put(select.getString("value"), select);
                                    optionItem2.put(select.getString("apiKey"), select);
                                    optionItem3.put(select.getString("label"), select);
                                }
                            }
                        }
                        if ("multipicklist".equals(fieldItem.getString("type"))) {
                            JSONArray checkItem = fieldItem.getJSONArray("checkitem");
                            if (checkItem != null && checkItem.size() > 0) {
                                valueOption.put(apiKey, optionItem1);
                                apiKeyOption.put(apiKey, optionItem2);
                                labelOption.put(apiKey, optionItem3);
                                for (Object checkItemObject : checkItem) {
                                    JSONObject item = (JSONObject) checkItemObject;
                                    JSONObject check = new JSONObject();
                                    check.put("label", item.getString("label"));
                                    check.put("value", item.getString("value"));
                                    check.put("apiKey", item.getString("apiKey"));
                                    optionItem1.put(check.getString("value"), check);
                                    optionItem2.put(check.getString("apiKey"), check);
                                    optionItem3.put(check.getString("label"), check);
                                }
                            }
                        }
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return queryEntityFieldsAndItemOptionValue(client, entityName, mainKey);
                } else {
                    logger.error("queryEntityFieldsAndItemOptionValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
                }
            }
        } catch (Exception e) {
            logger.error("queryEntityFieldsAndItemOptionValue 报错信息：" + e.getMessage(), e);
        }
        return record;
    }


    /**
     * 查询实体选项值
     *
     * @param client     RkhdHttpClient类型的连接器-null时自动生成
     * @param entityName 对象名
     * @param mainKey    eg:label\value\apiKey
     * @return JSONObject
     */
    public static JSONObject queryItemOptionValue(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/description")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  entityName:" + entityName);
            if (response != null && response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONObject dataObject = response.getJSONObject("data");
                    JSONArray fields = dataObject.getJSONArray("fields");
                    for (Object fieldItem : fields) {
                        JSONObject field = (JSONObject) fieldItem;
                        String apiKey = field.getString("apiKey");
                        JSONObject option = new JSONObject();
                        if ("picklist".equals(field.getString("type"))) {
                            JSONArray selectItem = field.getJSONArray("selectitem");
                            if (selectItem.size() > 0) {
                                record.put(apiKey, option);
                                for (Object itemObject : selectItem) {
                                    JSONObject item = (JSONObject) itemObject;
                                    JSONObject select = new JSONObject();
                                    select.put("label", item.getString("label"));
                                    select.put("value", item.getString("value"));
                                    select.put("apiKey", item.getString("apiKey"));
                                    JSONArray dependentValue = item.getJSONArray("dependentValue");
                                    if (dependentValue != null && dependentValue.size() > 0) {
                                        JSONObject vObject = new JSONObject();
                                        for (Object v : dependentValue) {
                                            vObject.put(v.toString(), v.toString());
                                        }
                                        String vStr = JSONObject.toJSONString(vObject.keySet());
                                        select.put("dependentValue", JSONArray.parseArray(vStr));
                                    }
                                    option.put(select.getString(mainKey), select);
                                }
                            }
                        }
                        if ("multipicklist".equals(field.getString("type"))) {
                            JSONArray checkItem = field.getJSONArray("checkitem");
                            if (checkItem != null && checkItem.size() > 0) {
                                record.put(apiKey, option);
                                for (Object checkItemObject : checkItem) {
                                    JSONObject item = (JSONObject) checkItemObject;
                                    JSONObject check = new JSONObject();
                                    check.put("label", item.getString("label"));
                                    check.put("value", item.getString("value"));
                                    check.put("apiKey", item.getString("apiKey"));
                                    option.put(check.getString(mainKey), check);
                                }
                            }
                        }
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return queryItemOptionValue(client, entityName, mainKey);
                } else {
                    logger.error("queryItemOptionValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
                }
            } else {
                logger.error("queryItemOptionValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
            }
        } catch (Exception e) {
            logger.error("queryItemOptionValue 报错信息：" + e.getMessage(), e);
        }
        return record;
    }

    /**
     * 查询实体所有选项值(包含业务类型)
     *
     * @param client     RkhdHttpClient类型的连接器-null时自动生成
     * @param entityName 对象名
     * @param mainKey    eg:label\value
     * @return JSONObject
     */
    public static JSONObject queryAllItemOptionValue(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject option = queryItemOptionValue(client, entityName, mainKey);
        JSONObject entityTypeOption = queryEntityTypeValue(client, entityName, mainKey);
        option.put("entityType", entityTypeOption);
        return option;
    }

    //region 选项值转换

    /**
     * 选项值转换
     *
     * @param option  选项集合
     * @param field   字段ApiKey
     * @param mainKey eg:label\value\apiKey
     * @param value   转换前的值
     * @return 转换后的值
     */
    public static String convertItemOption(JSONObject option, String field, String mainKey, String value) {
        if (StringUtils.isNotBlank(value)) {
            if (option.containsKey(field)) {
                if (option.getJSONObject(field).containsKey(value)) {
                    return option.getJSONObject(field).getJSONObject(value).getString(mainKey);
                }
            }
            if (StringUtils.isBlank(field)) {
                if (option.containsKey(value)) {
                    return option.getJSONObject(value).getString(mainKey);
                }
            }
        }
        return "";
    }

    public static String convertItemOption(JSONObject option, String field, String mainKey, Integer value) {
        if (value != null) {
            return convertItemOption(option, field, mainKey, value.toString());
        }
        return "";
    }

    public static String convertItemOption(JSONObject option, String field, String mainKey, Integer[] value) {
        if (value != null) {
            List<String> str = new ArrayList<>();
            for (Integer i : value) {
                str.add(convertItemOption(option, field, mainKey, i.toString()));
            }
            return String.join(",", str);
        }
        return "";
    }

    public static String convertItemOption(JSONObject option, String field, String mainKey, Long value) {
        if (value != null) {
            return convertItemOption(option, field, mainKey, value.toString());
        }
        return "";
    }

    public static Long convertItemOptionLong(JSONObject option, String field, String mainKey, String value) {
        String result = convertItemOption(option, field, mainKey, value);
        if (StringUtils.isNotBlank(result)) {
            return Long.valueOf(result);
        }
        return null;
    }

    public static Integer convertItemOptionInteger(JSONObject option, String field, String mainKey, String value) {
        String result = convertItemOption(option, field, mainKey, value);
        if (StringUtils.isNotBlank(result)) {
            return Integer.valueOf(result);
        }
        return null;
    }

    public static Double convertItemOptionDouble(JSONObject option, String field, String mainKey, String value) {
        String result = convertItemOption(option, field, mainKey, value);
        if (StringUtils.isNotBlank(result)) {
            return Double.valueOf(result);
        }
        return null;
    }

    public static Integer[] convertItemOptionIntegerArray(JSONObject option, String field, String mainKey, String value) {
        if (StringUtils.isNotBlank(value)) {
            List<Integer> result = new ArrayList<>();
            for (String key : value.split(",")) {
                Integer integer = convertItemOptionInteger(option, field, mainKey, key);
                if (integer != null) {
                    result.add(integer);
                }
            }
            return result.toArray(new Integer[0]);
        }
        return null;

    }

    //endregion

    /**
     * 查询实体业务类型
     *
     * @param client     RkhdHttpClient类型的连接器-null时自动生成
     * @param entityName 对象名
     * @param mainKey    eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject queryEntityTypeValue(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/busiType")
                    .header("xsy-criteria", "10")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  entityName:" + entityName + " mainKey:" + mainKey);
            if (response != null && response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONObject dataObject = response.getJSONObject("data");
                    JSONArray entityTypes = dataObject.getJSONArray("records");
                    for (int i = 0; i < entityTypes.size(); i++) {
                        JSONObject item = entityTypes.getJSONObject(i);
                        JSONObject entityType = new JSONObject();
                        entityType.put("label", item.getString("label"));
                        entityType.put("apiKey", item.getString("apiKey"));
                        entityType.put("value", item.getString("id"));
                        record.put(entityType.getString(mainKey), entityType);
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return queryEntityTypeValue(client, entityName, mainKey);
                } else {
                    logger.error("queryEntityTypeValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
                }
            } else {
                logger.error("queryEntityTypeValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
            }
        } catch (Exception e) {
            logger.error("queryEntityTypeValue error :" + e.getMessage(), e);
        }
        return record;
    }


    public static JSONObject queryEntityTypeValue(RkhdHttpClient client, Long belongId, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/data/v1/objects/customize/describe?belongId=" + belongId)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  belongId:" + belongId + " mainKey:" + mainKey);
            if (response != null && response.containsKey("entityTypes")) {
                JSONArray entityTypes = response.getJSONArray("entityTypes");
                for (int i = 0; i < entityTypes.size(); i++) {
                    JSONObject item = entityTypes.getJSONObject(i);
                    JSONObject entityType = new JSONObject();
                    entityType.put("label", item.getString("name"));
                    entityType.put("apiKey", item.getString("name"));
                    entityType.put("value", item.getString("id"));
                    record.put(entityType.getString(mainKey), entityType);
                }
            } else {
                logger.error("queryEntityTypeValue 未查询到结果：" + JSONObject.toJSONString(response) + "belongId:" + belongId);
            }
        } catch (Exception e) {
            logger.error("queryEntityTypeValue error :" + e.getMessage(), e);
        }
        return record;
    }


    /**
     * 查询商机阶段信息
     *
     * @param client           RkhdHttpClient类型的连接器-null时自动生成
     * @param entityTypeApiKey 业务类型ApiKey
     * @param mainKey          eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject querySaleStageValue(RkhdHttpClient client, String entityTypeApiKey, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            JSONObject object = new JSONObject();
            object.put("entityTypeApiKey", entityTypeApiKey);
            param.put("data", object);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("Post")
                    .callString("/rest/data/v2.0/xobjects/stage/actions/getStageListByEntityTypeApiKey")
                    .header("xsy-criteria", "10")
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  entityTypeApiKey:" + entityTypeApiKey + " mainKey:" + mainKey);
            if (response != null && response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONArray dataArray = response.getJSONArray("data");
                    for (int i = 0; i < dataArray.size(); i++) {
                        JSONObject item = dataArray.getJSONObject(i);
                        JSONObject stage = new JSONObject();
                        stage.put("label", item.getString("stageName"));
                        stage.put("apiKey", item.getString("stageName_resourceKey"));
                        stage.put("value", item.getString("id"));
                        stage.put("winRate", item.getDouble("percent").toString());
                        stage.put("order", item.getInteger("order").toString());
                        record.put(stage.getString(mainKey), stage);
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return querySaleStageValue(client, entityTypeApiKey, mainKey);
                } else {
                    logger.error("querySaleStageValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityTypeApiKey:" + entityTypeApiKey);
                }
            } else {
                logger.error("querySaleStageValue 未查询到结果：" + JSONObject.toJSONString(response) + "entityTypeApiKey:" + entityTypeApiKey);
            }
        } catch (Exception e) {
            logger.error("querySaleStageValue error :" + e.getMessage(), e);
        }
        return record;
    }

    /**
     * 查询BelongId列表
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:apiKey\value\label
     */
    public static JSONObject queryBelongIdValue(RkhdHttpClient client, String mainKey) {
        JSONObject record = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("Get")
                    .callString("/rest/metadata/v2.0/xobjects/filter")
                    .header("xsy-criteria", "10")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response));
            if (response != null && response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == 0) {
                JSONArray dataArray = response.getJSONObject("data").getJSONArray("records");
                for (int i = 0; i < dataArray.size(); i++) {
                    JSONObject item = dataArray.getJSONObject(i);
                    JSONObject xobjects = new JSONObject();
                    xobjects.put("label", item.getString("label"));
                    xobjects.put("apiKey", item.getString("apiKey"));
                    xobjects.put("value", item.getString("objectId"));
                    record.put(xobjects.getString(mainKey), xobjects);
                }
            }
        } catch (Exception e) {
            logger.error("queryBelongIdValue error :" + e.getMessage(), e);
        }
        return record;
    }

    /**
     * 获取业务类型ID
     *
     * @param client     RkhdHttpClient类型的连接器-null时自动生成
     * @param entityName 对象名
     * @param apiKey     业务类型 apiKey
     * @return 业务类型ID
     */
    public static Long getEntityTypeId(RkhdHttpClient client, String entityName, String apiKey) {
        JSONObject object;
        if ("user".equals(entityName)) {
            object = queryEntityTypeValue(client, 70L, "apiKey");
        } else {
            object = queryEntityTypeValue(client, entityName, "apiKey");
        }
        if (object.containsKey(apiKey)) {
            return object.getJSONObject(apiKey).getLong("value");
        }
        return null;
    }

    /**
     * 修改实体信息
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名
     * @param id         记录ID
     * @param object     修改纪录的信息
     * @return boolean
     */
    public static boolean updateEntity(RkhdHttpClient client, String entityName, long id, JSONObject object) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            param.put("data", object);
            String url = "/rest/data/v2.0/xobjects/" + entityName + "/" + id;
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("PATCH")
                    .callString(url)
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("修改实体信息:返回信息：" + response.toJSONString() + " 实体名:" + entityName + " 修改记录ID ：" + id + "，修改内容：" + param);
            Integer responseCode = response.getIntValue(CODE_STR);
            if (RESULT_CODE.equals(responseCode)) {
                return true;
            } else if (USER_FREQUENCY_LIMIT_ERROR.equals(responseCode)) {
                threadSleep();
                return updateEntity(client, entityName, id, object);
            } else {
                logger.error("updateEntity error：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
            }
        } catch (Exception e) {
            logger.error("updateEntity error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 修改实体信息V1
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名
     * @param param      修改纪录的信息
     * @return boolean
     */
    public static boolean updateEntityV1(RkhdHttpClient client, String entityName, JSONObject param) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            String url = "/data/v1/objects/" + entityName + "/update";
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("Post")
                    .callString(url)
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            logger.debug("修改实体信息，实体名：" + entityName + "，修改内容：" + param);
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            Integer responseCode = response.getInteger("status");
            if (STATUS_CODE.equals(responseCode)) {
                return true;
            }
        } catch (Exception e) {
            logger.error("updateEntity error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 创建实体信息
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名
     * @param object     纪录的信息
     * @return long 实体ID
     */
    public static long createEntity(RkhdHttpClient client, String entityName, JSONObject object) {
        long resultId = 0;
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            param.put("data", object);
            String url = "/rest/data/v2.0/xobjects/" + entityName;
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString(url)
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("新增实体信息:返回信息：" + response.toJSONString() + " 实体名:" + entityName + " 新增内容：" + param);
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    if (response.containsKey("data")) {
                        JSONObject result = response.getJSONObject("data");
                        if (result.containsKey("id") && result.getLong("id") != null) {
                            resultId = result.getLong("id");
                        }
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return createEntity(client, entityName, object);
                } else {
                    logger.error("createEntity error：" + JSONObject.toJSONString(response) + "entityName:" + entityName);
                }
            }
        } catch (Exception e) {
            logger.error("createEntity error ：" + e.getMessage(), e);
        }
        return resultId;
    }

    /**
     * 创建实体信息V1
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名
     * @param object     纪录的信息
     * @return long 实体ID
     */
    public static long createEntityV1(RkhdHttpClient client, String entityName, JSONObject object) {
        long resultId = 0;
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            param.put("record", object);
            String url = "/data/v1/objects/" + entityName + "/create";
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString(url)
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            logger.debug("新增实体信息，实体名：" + entityName + " 内容：" + param);
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey("id")) {
                resultId = response.getLong("id");
            }
        } catch (Exception e) {
            logger.error(" 报错信息：" + e);
        }
        return resultId;
    }

    //region 审批流相关

    /**
     * 审批流预处理
     *
     * @param client       RkhdHttpClient
     * @param entityApiKey 实体名
     * @param action       操作类型  submit:提交;reject:拒绝;agree:同意
     * @param dataId       数据ID
     * @return JSONObject
     */
    public static JSONObject preProcessor(RkhdHttpClient client, String entityApiKey, String action, Long dataId, Long userTaskLogId) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            JSONObject object = new JSONObject();
            param.put("data", object);
            object.put("entityApiKey", entityApiKey);
            object.put("action", action);
            object.put("dataId", dataId);
            if (userTaskLogId != null) {
                object.put("usertaskLogId", userTaskLogId);
            }
            String url = "/rest/data/v2.0/creekflow/task/actions/preProcessor";
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString(url)
                    .body(param.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    return response;
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return preProcessor(client, entityApiKey, action, dataId, userTaskLogId);
                } else {
                    logger.error("preProcessor error：" + JSONObject.toJSONString(response) + "参数:" + param.toJSONString());
                }
            }
        } catch (Exception e) {
            logger.error("preProcessor error ：" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 查询流程实例
     *
     * @param client  RkhdHttpClient
     * @param status  状态  processed:办理中;stop:中止;pass:通过
     * @param dataIds 数据ID
     * @return JSONObject
     */
    public static JSONObject processInstances(RkhdHttpClient client, String status, String dataIds) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            String url = "/rest/data/v2.0/creekflow/processInstances/filter?status=" + status + "&dataIds=" + dataIds;
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString(url)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                JSONObject data2 = response.getJSONObject("data");
                JSONArray records = data2.getJSONArray("records");
                if (records.size() > 0) {
                    return records.getJSONObject(records.size() - 1);
                }
            }
        } catch (Exception e) {
            logger.error("processInstances error ：" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 审批处理
     *
     * @param client       RkhdHttpClient
     * @param entityApiKey 实体名
     * @param action       操作类型    submit：提交;agree：同意;reject：拒绝;withdraw：撤回;plussign：加签;singleRegain：单步收回;turn：转办;taskSuspend：暂不处理;taskUnsuspend：恢复处理
     * @param dataId       数据ID
     * @return boolean
     */
    public static boolean creekFlowTask(RkhdHttpClient client, String entityApiKey, String action, Long dataId) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            JSONObject param = new JSONObject();
            JSONObject object = new JSONObject();
            param.put("data", object);
            object.put("entityApiKey", entityApiKey);
            object.put("action", action);
            object.put("dataId", dataId);

            if ("withdraw".equals(action)) {
                //撤回
                JSONObject processInstance = processInstances(client, "processed", dataId.toString());
                if (processInstance == null) {
                    throw new Exception("查询流程实例出错");
                }
                object.put("procdefId", processInstance.getLong("procId"));
                object.put("procInstId", processInstance.getLong("id"));
            }
            if ("agree".equals(action)) {
                JSONArray flowHistory = creekFlowHistory(client, entityApiKey, dataId, false);
                if (flowHistory == null || flowHistory.size() == 0) {
                    throw new Exception("查询流程历史");
                }
                object.put("usertaskLogId", flowHistory.getJSONObject(flowHistory.size() - 1).getLong("id"));
            }

            if ("submit".equals(action) || "agree".equals(action)) {
                JSONObject preResponse = preProcessor(client, entityApiKey, action, dataId, object.getLong("usertaskLogId"));
                if (preResponse == null) {
                    throw new Exception("预处理出错");
                }
                if (preResponse.containsKey("data")) {
                    preResponse = preResponse.getJSONObject("data");
                }
                object.put("procdefId", preResponse.getLong("procdefId"));
                object.put("nextTaskDefKey", preResponse.getString("nextTaskDefKey"));
                JSONArray chooseApprovers = preResponse.getJSONArray("chooseApprover");
                if (chooseApprovers != null && chooseApprovers.size() > 0) {
                    JSONArray nextAssignees = new JSONArray();
                    for (int i = 0; i < chooseApprovers.size(); i++) {
                        nextAssignees.add(chooseApprovers.getJSONObject(i).getLong("id"));
                    }
                    object.put("nextAssignees", nextAssignees);
                }
            }

            String url = "/rest/data/v2.0/creekflow/task";
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString(url)
                    .body(param.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    return true;
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return creekFlowTask(client, entityApiKey, action, dataId);
                } else {
                    logger.error("creekFlowTask error：" + JSONObject.toJSONString(response) + "参数:" + param.toJSONString());
                }
            }
        } catch (Exception e) {
            logger.error("creekFlowTask error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 查询审批流历史
     *
     * @param client   RkhdHttpClient
     * @param dataId   数据ID
     * @param stageFlg 是否阶段审批
     * @return JSONArray
     */
    public static JSONArray creekFlowHistory(RkhdHttpClient client, String entityApiKey, Long dataId, boolean stageFlg) {
        JSONArray result = new JSONArray();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            String url = "/rest/data/v2.0/creekflow/history/filter?entityApiKey=" + entityApiKey + "&dataId=" + dataId + "&stageFlg=" + stageFlg;
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString(url)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    result = response.getJSONArray("data");
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return creekFlowHistory(client, entityApiKey, dataId, stageFlg);
                } else {
                    logger.error("creekFlowHistory error：" + JSONObject.toJSONString(response) + "url:" + url);
                }
            }
        } catch (Exception e) {
            logger.error("creekFlowHistory error ：" + e.getMessage(), e);
        }
        return result;
    }


    //endregion

    /**
     * 查询团队成员
     *
     * @param client     RkhdHttpClient
     * @param entityName 实体名
     * @param recordId   数据ID
     * @param userId     用户ID  为null时 查询记录下所有团队成员
     */
    public static JSONArray queryTeamMember(RkhdHttpClient client, String entityName, Long recordId, Long userId) {
        JSONArray record = new JSONArray();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            String url = "/rest/data/v2.0/xobjects/teamMember/actions/list?xObjectApiKey=" + entityName + "&recordId=" + recordId + "&status=all";
            if (userId != null) {
                url = "/rest/data/v2.0/xobjects/teamMember?xObjectApiKey=" + entityName + "&recordId=" + recordId + "&userId=" + userId;
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString(url)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + JSONObject.toJSONString(response) + "  url:" + url);
            if (response != null && response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    String dataStr = response.getString("data");
                    if (StringUtils.isNotBlank(dataStr)) {
                        if (dataStr.indexOf("[") == 0) {
                            JSONArray dataArray = JSONArray.parseArray(dataStr);
                            for (int i = 0; i < dataArray.size(); i++) {
                                JSONObject dataJson = dataArray.getJSONObject(i);
                                JSONObject userJson = dataJson.getJSONObject("userId");
                                dataJson.put("userId", userJson.getLong("id"));
                                record.add(dataJson);
                            }
                        } else {
                            JSONObject dataJson = JSONObject.parseObject(dataStr);
                            JSONObject userJson = dataJson.getJSONObject("userId");
                            dataJson.put("userId", userJson.getLong("id"));
                            record.add(dataJson);
                        }
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return queryTeamMember(client, entityName, recordId, userId);
                } else {
                    logger.error("queryTeamMember 未查询到结果：" + JSONObject.toJSONString(response) + " url:" + url);
                }
            }
        } catch (Exception e) {
            logger.error("queryTeamMember error ：" + e.getMessage(), e);
        }
        return record;
    }

    /**
     * @param client    RkhdHttpClient类型的连接器-null时自动生成
     * @param url       url
     * @param method    post/get
     * @param parameter 参数
     * @param type      1:配置xml类型 2：注解类型
     */
    public static JSONObject callApi(RkhdHttpClient client, String url, String method, JSONObject parameter, int type) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            HashMap<String, Object> formData = new HashMap<>(1);
            for (String key : parameter.keySet()) {
                formData.put(key, parameter.getString(key));
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType(method)
                    .callString(url)
                    .body(parameter.toJSONString())
                    .build();
            if (type == 1) {
                data.setFormData(formData);
            }
            String responseStr = client.execute(data, ResponseBodyHandlers.ofString());
            if (StringUtils.isNotBlank(responseStr)) {
                //不改变JSON key的顺序
                JSONObject response = JSONObject.parseObject(responseStr, Feature.OrderedField);
                logger.debug("返回信息：" + response);
                return response;
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.callApi:" + e.getMessage());
        }
        return new JSONObject();
    }

    public static JSONObject callApi(RkhdHttpClient client, String url, String method, JSONArray parameter) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType(method)
                    .callString(url)
                    .body(parameter.toJSONString())
                    .build();
            String responseStr = client.execute(data, ResponseBodyHandlers.ofString());
            if (StringUtils.isNotBlank(responseStr)) {
                //不改变JSON key的顺序
                JSONObject response = JSONObject.parseObject(responseStr, Feature.OrderedField);
                logger.debug("返回信息：" + response);
                return response;
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.callApi:" + e.getMessage());
        }
        return new JSONObject();
    }

    /**
     * 通知消息
     *
     * @param client   RkhdHttpClient
     * @param belongId 实体ID
     * @param dataId   数据ID
     * @param user     用户
     * @param content  通知信息 rg:"项目授权管理-提交校验数据唯一性不通过，点击{arg0}查看"
     */
    public static boolean notice(RkhdHttpClient client, long belongId, long dataId, List<Long> user, String content) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            JSONObject noticeObject = new JSONObject();
            noticeObject.put("belongId", belongId);
            noticeObject.put("content", content);
            JSONArray mergeFields = new JSONArray();
            JSONObject mergeFieldsObject = new JSONObject();
            mergeFieldsObject.put("belongId", belongId);
            mergeFieldsObject.put("objectId", dataId);
            mergeFieldsObject.put("type", 1);
            mergeFields.add(mergeFieldsObject);
            noticeObject.put("mergeFields", mergeFields);
            noticeObject.put("objectId", dataId);
            JSONArray receivers = new JSONArray();
            for (Long userId : user) {
                JSONObject receiversObject = new JSONObject();
                receiversObject.put("id", userId);
                receiversObject.put("receiverType", 0);
                receivers.add(receiversObject);
            }
            noticeObject.put("receivers", receivers);

            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("post")
                    .callString("/rest/notice/v2.0/newNotice")
                    .body(noticeObject.toJSONString())
                    .build();

            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response + " 参数：" + noticeObject.toJSONString());
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    return true;
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return notice(client, belongId, dataId, user, content);
                } else {
                    logger.error("RkhdHelper.notice error：" + JSONObject.toJSONString(response) + " 参数:" + noticeObject.toJSONString());
                }
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.notice:" + e.getMessage(), e);
        }
        return false;
    }


    //region 文档相关处理
    public static Long twitterFileSaveDirectory(RkhdHttpClient client, long belongId, long dataId, String directoryName) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject body = new JSONObject();
            body.put("isDepartmentId", false);
            body.put("belongId", belongId);
            body.put("objectId", dataId);
            body.put("parentDirectoryId", -1);
            JSONObject directoryJsonData = new JSONObject();
            body.put("directoryJsonData", directoryJsonData);
            directoryJsonData.put("dirName", directoryName);
            directoryJsonData.put("disObjType", 1);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("post")
                    .callString("/rest/data/v2.0/xobjects/twitterFile/actions/saveDirectory")
                    .body(body.toJSONString())
                    .build();

            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response);
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                JSONObject result = response.getJSONObject("result");
                return result.getLong("id");
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.twitterFileSaveDirectory:" + e.getMessage(), e);
        }
        return null;
    }

    public static Long createDocument(RkhdHttpClient client, long belongId, long dataId, long fileId, long directoryId) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject body = new JSONObject();
            JSONObject record = new JSONObject();
            body.put("record", record);
            record.put("id", fileId);
            record.put("belongId", belongId);
            record.put("objectId", dataId);
            record.put("directoryId", directoryId);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("post")
                    .callString("/data/v1/objects/document/create")
                    .body(body.toJSONString())
                    .build();

            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response);
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                JSONObject result = response.getJSONObject("result");
                return result.getLong("id");
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.createDocument:" + e.getMessage(), e);
        }
        return null;
    }

    public static List<Long> twitterFileSaveFile(RkhdHttpClient client, long belongId, long dataId, long directoryId, JSONArray files) {
        List<Long> documentIds = new ArrayList<>();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject body = new JSONObject();
            body.put("pid", directoryId);
            body.put("belongId", belongId);
            body.put("objectId", dataId);
            body.put("position", "rescenter");
            body.put("objType", 0);
            body.put("isObj", false);
            body.put("groupId", -1);
            body.put("fileList", files);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("post")
                    .callString("/rest/data/v2.0/xobjects/twitterFile/actions/saveFile")
                    .body(body.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response);
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                JSONObject result = response.getJSONObject("result");
                JSONArray fileid = result.getJSONArray("fileid");
                if (fileid != null) {
                    for (int i = 0; i < fileid.size(); i++) {
                        documentIds.add(fileid.getJSONObject(i).getLong("id"));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("RkhdHelper.twitterFileSaveFile:" + e.getMessage(), e);
        }
        return documentIds;
    }

    //endregion

    //region  用户相关接口

    /**
     * 用户离职接口
     *
     * @param client RkhdHttpClient
     * @param userId 用户ID
     * @return boolean
     */
    public static boolean userDeparture(RkhdHttpClient client, long userId) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject updateData = new JSONObject();
            updateData.put("id", userId);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString("/data/v1/objects/user/departure")
                    .body(updateData.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (STATUS_CODE.equals(response.getInteger("status"))) {
                return true;
            }
        } catch (Exception e) {
            logger.error("userDeparture error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 授权删除
     *
     * @param client RkhdHttpClient
     * @param userId 用户ID
     * @return boolean
     */
    public static boolean licenseDelete(RkhdHttpClient client, long userId) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject updateData = new JSONObject();
            updateData.put("userId", userId);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString("/data/v1/objects/user/license/delete")
                    .body(updateData.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (STATUS_CODE.equals(response.getInteger("status"))) {
                return true;
            }
        } catch (Exception e) {
            logger.error("licenseDelete error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 查询角色列表
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject roleList(RkhdHttpClient client, String mainKey) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/data/v1/objects/role/list")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey("record")) {
                JSONArray record = response.getJSONArray("record");
                JSONObject result = new JSONObject();
                for (int i = 0; i < record.size(); i++) {
                    JSONObject item = record.getJSONObject(i);
                    JSONObject option = new JSONObject();
                    option.put("label", item.getString("name"));
                    option.put("value", item.getString("id"));
                    option.put("apiKey", item.getString("code"));
                    result.put(option.getString(mainKey), option);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("roleList error ：" + e.getMessage(), e);
        }
        return new JSONObject();
    }

    /**
     * 角色添加
     *
     * @param client   RkhdHttpClient
     * @param userId   用户ID
     * @param roleName 角色名称
     * @return boolean
     */
    public static boolean roleCreate(RkhdHttpClient client, long userId, String roleName) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject roles = roleList(client, "label");
            return roleCreate(client, userId, roleName, roles);
        } catch (Exception e) {
            logger.error("roleCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * @param client   RkhdHttpClient
     * @param userId   用户ID
     * @param roleName 角色名称
     * @param roles    角色列表
     * @return boolean
     */
    public static boolean roleCreate(RkhdHttpClient client, long userId, String roleName, JSONObject roles) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            if (roles.containsKey(roleName)) {
                JSONObject param = new JSONObject();
                JSONObject object = new JSONObject();
                param.put("record", object);
                object.put("userId", userId);
                object.put("roleId", RkhdHelper.convertItemOption(roles, null, "value", roleName));
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("POST")
                        .callString("/data/v1/objects/user/role/create")
                        .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + response.toJSONString());
                Integer responseCode = response.getInteger("status");
                if (STATUS_CODE.equals(responseCode)) {
                    return true;
                }

                if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return roleCreate(client, userId, roleName, roles);
                }

            } else {
                throw new RuntimeException("角色名称不存在：" + roleName);
            }
        } catch (Exception e) {
            logger.error("roleCreate error ：" + e.getMessage(), e);
        }
        return false;
    }


    /**
     * 查询职能列表
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject responsibilityList(RkhdHttpClient client, String mainKey) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/data/v1/objects/responsibility/list")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey("record")) {
                JSONArray record = response.getJSONArray("record");
                JSONObject result = new JSONObject();
                for (int i = 0; i < record.size(); i++) {
                    JSONObject item = record.getJSONObject(i);
                    JSONObject option = new JSONObject();
                    option.put("label", item.getString("name"));
                    option.put("value", item.getString("id"));
                    option.put("apiKey", item.getString("code"));
                    result.put(option.getString(mainKey), option);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("responsibilityList error ：" + e.getMessage(), e);
        }
        return new JSONObject();
    }

    /**
     * 用户职能添加
     *
     * @param client   RkhdHttpClient
     * @param userId   用户ID
     * @param roleName 职能名称
     * @return boolean
     */
    public static boolean userResponsibilitiesCreate(RkhdHttpClient client, long userId, String roleName) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject roles = responsibilityList(client, "label");
            return userResponsibilitiesCreate(client, userId, roleName, roles);

        } catch (Exception e) {
            logger.error("roleCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 用户职能添加
     *
     * @param client   RkhdHttpClient
     * @param userId   用户ID
     * @param roleName 职能名称
     * @param roles    职能列表
     * @return boolean
     */
    public static boolean userResponsibilitiesCreate(RkhdHttpClient client, long userId, String roleName, JSONObject roles) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            if (roles.containsKey(roleName)) {
                JSONObject param = new JSONObject();
                JSONObject object = new JSONObject();
                param.put("record", object);
                object.put("userId", userId);
                object.put("responsibilityId", RkhdHelper.convertItemOption(roles, null, "value", roleName));
                object.put("isPrimary", true);
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("POST")
                        .callString("/data/v1/objects/userresponsibilities/create")
                        .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + response.toJSONString());
                Integer responseCode = response.getInteger("status");
                if (STATUS_CODE.equals(responseCode)) {
                    return true;
                }
                if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return userResponsibilitiesCreate(client, userId, roleName, roles);
                }
            } else {
                throw new RuntimeException("角色名称不存在：" + roleName);
            }
        } catch (Exception e) {
            logger.error("roleCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 用户职能更新
     *
     * @param client   RkhdHttpClient
     * @param userId   用户ID
     * @param roleName 职能名称
     * @param roles    职能列表
     * @return boolean
     */
    public static boolean userResponsibilitiesUpdate(RkhdHttpClient client, long userId, String roleName, JSONObject roles) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            if (roles.containsKey(roleName)) {
                JSONObject param = new JSONObject();
                JSONObject object = new JSONObject();
                param.put("record", object);
                object.put("userId", userId);
                object.put("responsibilityId", RkhdHelper.convertItemOption(roles, null, "value", roleName));
                object.put("isPrimary", true);
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("POST")
                        .callString("/data/v1/objects/userresponsibilities/update")
                        .body(JSONObject.toJSONString(object, SerializerFeature.WriteMapNullValue))
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + response.toJSONString());
                Integer responseCode = response.getInteger("status");
                if (STATUS_CODE.equals(responseCode)) {
                    return true;
                }
            } else {
                throw new RuntimeException("角色名称不存在：" + roleName);
            }
        } catch (Exception e) {
            logger.error("roleCreate error ：" + e.getMessage(), e);
        }
        return false;
    }


    /**
     * 查询授权列表
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject licenseList(RkhdHttpClient client, String mainKey) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString("/data/v1/objects/license/list")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey("records")) {
                JSONArray record = response.getJSONArray("records");
                JSONObject result = new JSONObject();
                for (int i = 0; i < record.size(); i++) {
                    JSONObject item = record.getJSONObject(i);
                    JSONObject option = new JSONObject();
                    option.put("label", item.getString("licenseName"));
                    option.put("value", item.getString("id"));
                    option.put("apiKey", item.getString("licenseKey"));
                    result.put(option.getString(mainKey), option);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("roleList error ：" + e.getMessage(), e);
        }
        return new JSONObject();
    }


    /**
     * license授权
     *
     * @param client         RkhdHttpClient
     * @param userId         用户ID
     * @param excludeLicense 排除的授权列表
     * @return boolean
     */
    public static boolean licenseCreate(RkhdHttpClient client, long userId, List<String> excludeLicense) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject roles = licenseList(client, "label");
            return licenseCreate(client, userId, excludeLicense, roles);
        } catch (Exception e) {
            logger.error("licenseCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * license授权
     *
     * @param client         RkhdHttpClient
     * @param userId         用户ID
     * @param excludeLicense 排除的授权列表
     * @param roles          license列表
     * @return boolean
     */
    public static boolean licenseCreate(RkhdHttpClient client, long userId, List<String> excludeLicense, JSONObject roles) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            for (String key : roles.keySet()) {
                try {
                    if (excludeLicense == null || excludeLicense.size() == 0 || !excludeLicense.contains(key)) {
                        JSONObject param = new JSONObject();
                        JSONObject object = new JSONObject();
                        param.put("record", object);
                        object.put("userId", userId);
                        object.put("licenseId", roles.getJSONObject(key).getString("value"));
                        RkhdHttpData data = RkhdHttpData.newBuilder()
                                .callType("POST")
                                .callString("/data/v1/objects/user/license/create")
                                .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                                .build();
                        JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                        logger.debug("返回信息：" + response.toJSONString() + " 授权：" + key);
                    }
                } catch (Exception ex) {
                    logger.error("licenseCreate error ：" + ex.getMessage(), ex);
                }
            }
        } catch (Exception e) {
            logger.error("licenseCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * license授权
     *
     * @param client   RkhdHttpClient
     * @param userIds  用户ID
     * @param licenses license列表
     * @return boolean
     */
    public static boolean assignLicences(RkhdHttpClient client, List<Long> userIds, List<Long> licenses) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            JSONObject param = new JSONObject();
            param.put("userIds", userIds);
            param.put("licenceIdList", licenses);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("PATCH")
                    .callString("/rest/data/v2.0/xobjects/user/assignLicences")
                    .body(JSONObject.toJSONString(param, SerializerFeature.WriteMapNullValue))
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString() + " 授权：" + param.toJSONString());
        } catch (Exception e) {
            logger.error("licenseCreate error ：" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * 激活用户
     *
     * @param client         RkhdHttpClient
     * @param userIds        用户ID列表
     * @param sendNoticeMail 是否发送邮箱
     * @return boolean
     */
    public static boolean userActivate(RkhdHttpClient client, List<Long> userIds, boolean sendNoticeMail) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            JSONObject param = new JSONObject();
            param.put("userIds", userIds);
            param.put("sendNoticeMail", sendNoticeMail);
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("PATCH")
                    .callString("/rest/data/v2.0/xobjects/user/batchActive")
                    .body(param.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString() + " 参数：" + param.toJSONString());
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == RESULT_CODE) {
                return true;
            }
        } catch (Exception e) {
            logger.error("userActivate error ：" + e.getMessage(), e);
        }
        return false;
    }

    //endregion

    /**
     * 查询公海池
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:apiKey\value\label
     * @return JSONObject
     */
    public static JSONObject getHighSeaList(RkhdHttpClient client, String entityName, String mainKey) {
        JSONObject result = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("Get")
                    .callString("/rest/data/v2.0/xobjects/" + entityName + "/actions/getHighSeaList")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR)) {
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    JSONArray batchData = response.getJSONArray("batchData");
                    for (int i = 0; i < batchData.size(); i++) {
                        JSONObject item = batchData.getJSONObject(i);
                        if (item.containsKey("data")) {
                            JSONObject option = new JSONObject();
                            option.put("label", item.getJSONObject("data").getString("name"));
                            option.put("value", item.getJSONObject("data").getString("id"));
                            result.put(option.getString(mainKey), option);
                        }
                    }
                } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                    threadSleep();
                    return getHighSeaList(client, entityName, mainKey);
                } else {
                    logger.error("getHighSeaList 未查询到结果：" + JSONObject.toJSONString(response));
                }
            }
        } catch (Exception e) {
            logger.error("getHighSeaList error ：" + e.getMessage(), e);
        }
        return result;
    }


    /**
     * 获取货币信息
     *
     * @param client  RkhdHttpClient
     * @param mainKey eg:code\label\currencyCode
     * @return JSONObject
     */
    public static JSONObject queryCurrencies(RkhdHttpClient client, String mainKey) {
        JSONObject result = new JSONObject();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            String url = "/rest/metadata/v2.0/settings/systemSettings/currencies";
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString(url)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (response.containsKey(CODE_STR) && response.getLongValue(CODE_STR) == 0) {
                JSONObject resultJsonObject = response.getJSONObject("data");
                int count = resultJsonObject.getIntValue("count");
                if (count > 0) {
                    JSONArray records = response.getJSONObject("data").getJSONArray("records");
                    for (int i = 0; i < records.size(); i++) {
                        JSONObject item = records.getJSONObject(i);
                        JSONObject option = new JSONObject();
                        option.put("label", item.getString("label"));
                        option.put("apiKey", item.getString("currencyCode"));
                        option.put("value", item.getString("code"));
                        option.put("rate", item.getDouble("rate"));
                        result.put(option.getString(mainKey), option);
                    }
                }
            } else {
                logger.error("queryCurrencies 未查询到结果：" + JSONObject.toJSONString(response));
            }
        } catch (Exception e) {
            logger.error("queryCurrencies error ：" + e.getMessage(), e);
        }
        return result;
    }

    public static JSONObject queryGlobalPicks(RkhdHttpClient client, String apiKey, String mainKey) {
        JSONObject result = new JSONObject();
        try {

            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            String url = "/rest/metadata/v2.0/settings/globalPicks/" + apiKey;
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("GET")
                    .callString(url)
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (STATUS_CODE.equals(response.getInteger(CODE_STR))) {
                JSONArray records = response.getJSONObject("data").getJSONObject("records").getJSONArray("pickOption");
                for (int i = 0; i < records.size(); i++) {
                    JSONObject item = records.getJSONObject(i);
                    JSONObject option = new JSONObject();
                    option.put("label", item.getString("optionLabel"));
                    option.put("apiKey", item.getString("optionApiKey"));
                    option.put("value", item.getString("optionCode"));
                    result.put(option.getString(mainKey), option);
                }
            } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                threadSleep();
                return queryGlobalPicks(client, apiKey, mainKey);
            } else {
                logger.error("queryGlobalPicks 未查询到结果：" + JSONObject.toJSONString(response));
            }
        } catch (Exception e) {
            logger.error("queryGlobalPicks error ：" + e.getMessage(), e);
        }
        return result;
    }


    public static boolean batchJob(RkhdHttpClient client, String entityName, String operation, List<String> callBackClasses, JSONArray list) {
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }

            JSONObject param = new JSONObject();
            JSONObject record = new JSONObject();
            param.put("data", record);
            record.put("operation", operation);
            record.put("object", entityName);
            if (callBackClasses != null && callBackClasses.size() > 0) {
                record.put("callBackClasses", callBackClasses);
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("POST")
                    .callString("/rest/bulk/v2/job")
                    .body(param.toJSONString())
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                String jobId = response.getJSONObject("result").getString("id");
                record.clear();
                record.put("jobId", jobId);
                record.put("datas", list);
                data = RkhdHttpData.newBuilder()
                        .callType("POST")
                        .callString("/rest/bulk/v2/batch")
                        .body(param.toJSONString())
                        .build();
                response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + response.toJSONString());
                if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                    return true;
                }
            } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                threadSleep();
                return batchJob(client, entityName, operation, callBackClasses, list);
            } else {
                logger.error("batchJob 未查询到结果：" + JSONObject.toJSONString(response));
            }
        } catch (Exception e) {
            logger.error("batchJob error ：" + e.getMessage(), e);
        }
        return false;
    }


    public static JSONArray getBatchResult(RkhdHttpClient client, String batchId) {
        JSONArray successList = new JSONArray();
        try {
            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            RkhdHttpData data = RkhdHttpData.newBuilder()
                    .callType("Get")
                    .callString("/rest/bulk/v2/batch/" + batchId + "/result")
                    .build();
            JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
            logger.debug("返回信息：" + response.toJSONString());
            if (RESULT_CODE.equals(response.getInteger(CODE_STR))) {
                JSONObject result = response.getJSONObject("result");
                if (result != null && result.containsKey("records")) {
                    JSONArray records = result.getJSONArray("records");
                    if (records.size() > 0) {
                        successList.addAll(records);
                    }
                }
            } else if (USER_FREQUENCY_LIMIT_ERROR.equals(response.getInteger(CODE_STR))) {
                threadSleep();
                return getBatchResult(client, batchId);
            } else {
                logger.error("getBatchResult 未查询到结果：" + JSONObject.toJSONString(response));
            }

        } catch (Exception e) {
            logger.error("getBatchResult error ：" + e.getMessage(), e);
        }
        return successList;
    }

    public static JSONArray queryObjectFieldHistory(RkhdHttpClient client, JSONObject body) {
        JSONArray list = new JSONArray();
        try {

            if (client == null) {
                client = RkhdHttpClient.instance();
            }
            int pageNum = 1;
            int pages = 1;
            do {
                body.put("pageNum", pageNum);
                String url = "/rest/data/v2.0/logs/xobjectFieldHistory/actions/query";
                RkhdHttpData data = RkhdHttpData.newBuilder()
                        .callType("Post")
                        .callString(url)
                        .body(body.toJSONString())
                        .build();
                JSONObject response = client.execute(data, ResponseBodyHandlers.ofJSON());
                logger.debug("返回信息：" + response.toJSONString());
                if (response.containsKey("code") && response.getIntValue("code") == 0 && response.containsKey("result")) {
                    JSONObject result = response.getJSONObject("result");
                    pages = result.getIntValue("pages");
                    if (result.containsKey("records")) {
                        list.addAll(result.getJSONArray("records"));
                        pageNum++;
                    }
                }
            } while (pageNum <= pages);

        } catch (Exception e) {

        }
        return list;
    }



}
