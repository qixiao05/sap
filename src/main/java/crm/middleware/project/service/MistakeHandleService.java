package crm.middleware.project.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.sdk.http.rest.CommonRestClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MistakeHandleService {

    @Autowired
    CrmAPIs crmAPIs;
    @Autowired
    BulkService bulkService;
    @Autowired
    CommonRestClient commonRestClient;
    @Autowired
    EatonApiImpl eatonApi;

    public void hadleMissOrder(){
        JSONArray orders = crmAPIs.queryArrayV2("salesOrder__c","id,name","mistake__c=1",null);
        if(orders.size()>0){
            int index = 1;
            for(Object o:orders){
                log.info("total:"+orders.size()+" now:"+index);
                JSONObject order = (JSONObject) o;
                String DOC_NUMBER = order.getString("name");
                try {
                    eatonApi.querySalesOrderByOrderNo(DOC_NUMBER);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                index++;
            }
        }
    }
}
