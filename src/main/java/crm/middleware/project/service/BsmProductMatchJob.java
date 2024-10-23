package crm.middleware.project.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rkhd.platform.sdk.model.QueryResult;
import com.rkhd.platform.sdk.model.XObject;
import crm.middleware.project.sdk.CrmAPIs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class BsmProductMatchJob {

    @Autowired
    CrmAPIs crmAPIs;
    @Autowired
    BulkService bulkService;

    public void match(){
        JSONArray prodOrgs = crmAPIs.queryArrayV2("prodOrg__c","id,product__c,salesOgr__c,customItem12__c","salesOgr__c in ('5216','5249') and customItem11__c =2",null);

        if(prodOrgs.size()>0){
            int index =0;
            for(Object o :prodOrgs){
                index++;
                log.info("current:"+index+" total:"+prodOrgs.size());
                JSONObject prodOrg = (JSONObject)o;
                JSONObject update = new JSONObject();
                update.put("id",prodOrg.get("id"));
                if(null!=prodOrg.get("salesOgr__c")){
                    if("5216".equals(prodOrg.getString("salesOgr__c"))){
                        JSONArray bsmssproAndPrices = crmAPIs.queryArrayV2("bsmssproAndPrice__c","id","entityType = 3281706829284324 and product__c ="+prodOrg.get("product__c")+" and salesOrg__c = 7",null);
                        if(bsmssproAndPrices.size()==0){
                            JSONObject bsmssproAndPrice__c = new JSONObject();
                            bsmssproAndPrice__c.put("entityType",3281706829284324l);
                            bsmssproAndPrice__c.put("name",prodOrg.getString("customItem12__c"));
                            bsmssproAndPrice__c.put("product__c",prodOrg.get("product__c"));
                            bsmssproAndPrice__c.put("salesOrg__c",7);
                            JSONObject createResult = crmAPIs.createV2X("bsmssproAndPrice__c",bsmssproAndPrice__c);
                            log.info(createResult.toJSONString());
                            update.put("customItem11__c",1);
                            JSONObject updateResult = crmAPIs.updateV2X("prodOrg__c",update.getLong("id"),update);
                            log.info(updateResult.toJSONString());
                        }else {
                            update.put("customItem11__c",1);
                            JSONObject updateResult = crmAPIs.updateV2X("prodOrg__c",update.getLong("id"),update);
                            log.info(updateResult.toJSONString());
                        }
                    }else if("5249".equals(prodOrg.getString("salesOgr__c"))){
                        JSONArray bsmssproAndPrices = crmAPIs.queryArrayV2("bsmssproAndPrice__c","id","entityType = 3281706829284324 and product__c ="+prodOrg.get("product__c")+" and salesOrg__c = 8",null);
                        if(bsmssproAndPrices.size()==0){
                            JSONObject bsmssproAndPrice__c = new JSONObject();
                            bsmssproAndPrice__c.put("entityType",3281706829284324l);
                            bsmssproAndPrice__c.put("name",prodOrg.getString("customItem12__c"));
                            bsmssproAndPrice__c.put("product__c",prodOrg.get("product__c"));
                            bsmssproAndPrice__c.put("salesOrg__c",8);
                            JSONObject createResult = crmAPIs.createV2X("bsmssproAndPrice__c",bsmssproAndPrice__c);
                            log.info(createResult.toJSONString());
                            update.put("customItem11__c",1);
                            JSONObject updateResult = crmAPIs.updateV2X("prodOrg__c",update.getLong("id"),update);
                            log.info(updateResult.toJSONString());
                        }else {
                            update.put("customItem11__c",1);
                            JSONObject updateResult = crmAPIs.updateV2X("prodOrg__c",update.getLong("id"),update);
                            log.info(updateResult.toJSONString());
                        }
                    }
                }
            }

        }
    }
}
