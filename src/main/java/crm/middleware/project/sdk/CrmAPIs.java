package crm.middleware.project.sdk;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rkhd.platform.sdk.exception.XsyHttpException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description: 销售易openApi 封装
 * V2查询接口最大是100条，V1是200
 */
public interface CrmAPIs {

    /**
     * getToken,根据key取值
     *
     * @param key
     * @return
     */
    public String getToken(String key) throws XsyHttpException;

    /**
     * 查询固定分页数据
     *
     * @param sql
     * @return
     */
    public int doQueryByApiV2ByLimit(String sql, JSONArray arrays, int offset,Integer limit);
    /**
     * 自定义查询 Arry
     *
     * @param objectName   对象API
     * @param selectColumn 查询字段API
     * @param condition    查询条件
     * @param orders       排序字段
     * @return 返回 JSONArray（最大一次返回300条）
     */
    public JSONArray queryArrayV1(String objectName, String selectColumn, String condition, String orders);

    /**
     * 自定义查询 Map<String,String>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @param value         map value(String)
     * @return
     */
    public Map<String, String> queryMapV1(Set<String> set, String objectName, String selectColumns, String key, String value);

    /**
     * 自定义查询 Map<String,String>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @param limitNum      单次查询限制条数
     * @param value         map value(String)
     * @return
     */
    public Map<String, String> queryMapV1(Set<String> set, String objectName, String selectColumns, String key, String value,int limitNum);

    /**
     * 自定义查询 Map<String,JSONObject>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @param limitNum      单次查询限制条数
     * @return map(String, JSONObject)
     */
    public Map<String, JSONObject> queryMapV1(Set<String> set, String objectName, String selectColumns, String key,int limitNum);

    /**
     * 自定义查询 Map<String,JSONObject>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @return map(String, JSONObject)
     */
    public Map<String, JSONObject> queryMapV1(Set<String> set, String objectName, String selectColumns, String key);

    /**
     * 创建自定义对象v1接口: 返回null表示失败
     *
     * @param params   json
     * @param belongId
     * @return
     */
    public JSONObject createCustomizeV1(JSONObject params, long belongId);

    /**
     * 更新自定义对象v1接口
     *
     * @param params json
     * @return
     */
    public JSONObject updateCustomizeV1(JSONObject params);

    /**
     * 创建标准对象v1接口
     *
     * @param objectName
     * @param params
     * @return
     */
    public JSONObject createStandardV1(String objectName, JSONObject params);


    /**
     * 更新标准对象v1接口
     *
     * @param objectName
     * @param params
     * @return
     */
    public JSONObject updateStandardV1(String objectName, JSONObject params);

    /**
     * 对象明细数据查询V1接口，标准和自定义通用
     *
     * @param dataId
     * @param objectName 标准对象需带对象API
     * @return
     */
    public JSONObject queryObjectV1(long dataId, String objectName);

    /**
     * 对象删除V1接口，通用
     *
     * @param id
     * @param objectName 标准对象需带对象API
     * @return 返回null表示失败
     */
    public JSONObject deleteObjectV1(long id, String objectName);


    /**
     * V1对象描述接口， 通用
     *
     * @param objectName 标准对象需带对象API
     * @param belongId   自定义对象
     * @return
     */
    public JSONObject queryDescribeV1(String objectName, long belongId);

    /**
     * 团队成员管理
     *
     * @param businessId    数据id（dataId） t=0,1,2,3 必填
     * @param belongId      对象id t=0,1,3 必填
     * @param ownerFlag     是否是负责员工;值为1为负责员工，值为0为相关员工。负责员工为有修改权限的团队成员，相关员工为无修改权限的团队成员，不包含负责人 t=3 必填
     * @param users         用户数据集（user id） t=0,1 必填
     * @param xObjectApiKey 对象key t=2
     * @param t             类型：0 添加负责人
     *                      1 添加相关人
     *                      2 删除团队成员(负责人除外)
     *                      3 查询团队成员
     * @return
     */
    public JSONObject joinOwnerV1(Long businessId, Long belongId, String ownerFlag, JSONArray users, String xObjectApiKey, int t);

    /**
     * 创建站内消息
     *
     * @param targetUserid 接收方人员id
     * @param content      消息内容
     * @return
     */
    public JSONObject createNotify(String targetUserid, String content);

    /**
     * xoql查询，返回count
     *
     * @param sql
     * @return JSONObject
     */
    public JSONObject queryByXoqlApi(String sql) throws XsyHttpException;

    /**
     * V2 自定义查询，返回JSONArray
     *
     * @param objectName
     * @param selectColumn
     * @param condition
     * @param orders
     * @return jsonArray 最大100条
     */
    public JSONArray queryArrayV2(String objectName, String selectColumn, String condition, String orders);

    /**
     * V2 自定义查询 Map<String,JSONObject>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @return map(String, JSONObject)
     */
    public Map<String, JSONObject> queryMapV2(Set<String> set, String objectName, String selectColumns, String key);

    /**
     * V2 自定义查询 Map<String,String>
     *
     * @param set           in arrys
     * @param objectName    对象API
     * @param selectColumns 查询字段API
     * @param key           map ken
     * @return map(String, String)
     */
    public Map<String, String> queryMapV2(Set<String> set, String objectName, String selectColumns, String key,String value);

    /**
     * V2 创建接口
     *
     * @param xObjectApiKey
     * @param params
     * @return JSONObject
     * 返回示例: {"code":200,"msg":"操作成功","ext":[],"result": {"id": 17610}}
     */
    public JSONObject createV2(String xObjectApiKey, JSONObject params);

    /**
     * V2 创建接口
     *
     * @param xObjectApiKey
     * @param params
     * @return JSONObject
     * 返回示例: {"code":200,"msg":"操作成功","ext":[],"result": {"id": 17610}}
     */
    public JSONObject createV2X(String xObjectApiKey, JSONObject params);

    /**
     * V2 更新接口
     *
     * @param xObjectApiKey
     * @param params
     * @return JSONObject
     * 返回示例: {code": 200,"msg": "OK","data": {"id": 784156}}
     */
    public JSONObject updateV2(String xObjectApiKey, long id, JSONObject params);

    /**
     * V2 更新接口，可上传图片
     *
     * @param xObjectApiKey
     * @param params
     * @return JSONObject
     */
    public JSONObject updateV2X(String xObjectApiKey, long id, JSONObject params);

    /**
     * V2 删除接口
     *
     * @param xObjectApiKey
     * @param id
     * @return 返回示例: {code": 200,"msg": "OK","data": {"id": 784156}}
     */
    public JSONObject deleteV2(String xObjectApiKey, long id);

    /**
     * V2 删除接口
     *
     * @param xObjectApiKey
     * @param id
     */
    public JSONObject deleteV2X(String xObjectApiKey, long id);

    /**
     * V2 对象查询接口
     *
     * @param dataId
     * @param xObjectApiKey
     * @return 返回示例: {"id": 784156...}
     * getJSONObject("result")
     */
    public JSONObject queryObjectV2(long dataId, String xObjectApiKey);

    /**
     * V2 对象查询接口
     *
     * @param dataId
     * @param xObjectApiKey
     * @return 返回示例: {"id": 784156...}
     */
    public JSONObject queryObjectV2X(long dataId, String xObjectApiKey);

    /**
     * V2 描述接口
     *
     * @param xObjectApiKey 标准对象需带对象API
     * @return JSONObject
     * getJSONArray("fields");
     */
    public JSONObject queryDescribeV2(String xObjectApiKey);

    /**
     * 自定义V2接口
     *
     * @param url         访问URL全址
     * @param formData    map键值数据，默认为空
     * @param bodyData    json格式body数据
     * @param requetType  请求类型：get,post,delete,put,patch等
     * @param contentType 默认为null
     * @return
     */
    public JSONObject customizeV2(String url, Map<String, String> formData, JSONObject bodyData, String requetType, String contentType);

    /**
     * 创建dev帐户信息
     *
     * @return
     */
    public JSONObject createDevAccount();

    /**
     * 查询平台信息并做处理
     */
    //public void queryDevAccount();

    /**
     * 工商信息-根据公司名称模糊查询列表
     *
     * @param name
     * @return
     */
    public JSONObject queryEnterpriseList(String name);

    /**
     * 工商信息-根据模糊查询的公司名称查询精准企业信息
     *
     * @param name
     * @return
     */
    public JSONObject queryEnterpriseInfo(String name);

    /**
     * 发送通知 ( 支持新消息体结构 )简单，仅支持单个消息体，多接收人
     *
     * @param belongId         关联对象ID
     * @param objectId         关联数据ID
     * @param content          发送消息内容,示例： "股份下达了一个协同工作，主题为: " + cooperation.getString("title__c") + " 。请关注！，点击{arg0}查看";
     * @param ids              接收人ID(人，部门，群组)
     * @param mergeFieldsIndex 是从mergeFileds选取下标(从0开始)跳转 标识
     * @param receiverType     接收消息的类型(0：人，1：部门，2：群组)类型
     * @return
     */
    public JSONObject sendNotice(String belongId, String objectId, String content, Set<String> ids, Integer mergeFieldsIndex, Integer receiverType);

    /**
     * 创建异步作业
     * @param operation 执行的操作，目前支持： insert, update, delete, query
     * @param xObjectApiKey  操作的对象，可通过对象查询接口获取object name
     * @return
     */
    JSONObject createBulkJob(String operation,String xObjectApiKey);

    /**
     *
     * @param jobId 异步作业id
     * @param datas 批量实体数据参数
     * @return
     */
    JSONObject createBulkBatch(String jobId, List<JSONObject> datas);

    /**
     *
     * @param batchId 异步任务id
     * @return
     */
    JSONObject getBulkBatchResult(String batchId);

}
