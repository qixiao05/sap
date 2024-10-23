package crm.middleware.project.service;

import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.sdk.CrmAPIs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;


@Slf4j
@Component
public class BulkService {
    @Autowired
    CrmAPIs crmAPIs;

    /**
     * 创建异步作业
     * @param operation 执行的操作，目前支持： insert, update, delete, query
     * @param objectApiKey  操作的对象，可通过对象查询接口获取object name
     * @return
     */
    public String createBulkJob(String operation,String objectApiKey){
        JSONObject result = crmAPIs.createBulkJob(operation,objectApiKey);
        String jobId = result.getString("id");
        return jobId;
    }

    /**
     *
     * @param jobId 异步作业id
     * @param datas 批量实体数据参数
     * @return
     */
    public String createBulkBatch(String jobId, List<JSONObject> datas){
        JSONObject result = crmAPIs.createBulkBatch(jobId, datas);
        return result.getString("batchId");
    }

    /**
     *
     * @param batchId 异步任务id
     * @return
     */
    public void getBulkBatchResult(String batchId){
        JSONObject result = crmAPIs.getBulkBatchResult(batchId);

    }

}
