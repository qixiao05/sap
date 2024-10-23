package crm.middleware.project.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.cfg.CrmPropertiesConfig;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.util.DateUtil8;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Slf4j
@Component
public class BlockHandleService {

    @Autowired
    CrmPropertiesConfig crmPropertiesConfig;
    @Autowired
    CrmAPIs crmAPIs;
    @Autowired
    BulkService bulkService;

    public static void main(String[] args) {
        String updateTime = DateUtil8.getAfterOrPreNowTime(DateUtil8.getNowTime_EN(),"hour",-1l);
        Long time = DateUtil8.getTimeStamp(updateTime);
        System.out.println(time);
    }

    public void blockHandle(){

        JSONArray syncSettings = crmAPIs.queryArrayV2("syncSetting__c","id,name,accountType__c",null,null);
        JSONArray accounts = crmAPIs.queryArrayV2("account","id,sapid__c,cusTypeApi__c","accAlterFlag__c = 1",null);
        if(accounts.size()>0){
            List<JSONObject> update1 = new ArrayList<>();
            List<JSONObject> update2 = new ArrayList<>();
            for(Object a:accounts){
                if(syncSettings.size()>0){
                    JSONObject account = (JSONObject) a;
                    String entityType = account.getString("cusTypeApi__c");
                    String sapid__c = account.getString("sapid__c");
                    Set<String> orgs = new HashSet<>();

                    for(Object s:syncSettings){
                        JSONObject syncSetting = (JSONObject) s;
                        String accountType__c = null!=syncSetting.get("accountType__c")?syncSetting.getString("accountType__c"):null;
                        if(accountType__c.contains(entityType)){
                            orgs.add(syncSetting.getString("name"));
                        }
                    }
                    if(orgs.size()>0){
                        JSONArray accountOffices = crmAPIs.queryArrayV2("accountOffice__c","id,salesOrg__c,cusSAPCode__c,orderBlock__c,delivBlock__c,billingBlock__c","cusSAPCode__c = '"+sapid__c+ "' and salesOrg__c  in (" + StringUtils.join(orgs, ",") + ")", "");
                        for (Object accountOfficeObj:accountOffices){
                            JSONObject accountOffice = (JSONObject)accountOfficeObj;
                            if(null!=accountOffice.get("orderBlock__c") ||null!=accountOffice.get("delivBlock__c")||null!=accountOffice.get("billingBlock__c")){
                                account.put("salesViewBlock__c",1);
                                update2.add(account);
                                break;
                            }else {
                                account.put("salesViewBlock__c",2);
                                update1.add(account);
                            }
                        }
                    }
                    account.put("accAlterFlag__c",2);
                }

            }
            if(CollectionUtils.isNotEmpty(update1)){
                List<List<JSONObject>> updateLists = splitList(update1,5000);
                String updateJobId = bulkService.createBulkJob("update","account");
                int index = 1;
                for(List list : updateLists){
                    log.info("更新1 bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                    bulkService.createBulkBatch(updateJobId,list);
                    index++;
                }
            }

            if(CollectionUtils.isNotEmpty(update2)){
                List<List<JSONObject>> updateLists = splitList(update2,5000);
                String updateJobId = bulkService.createBulkJob("update","account");
                int index = 1;
                for(List list : updateLists){
                    log.info("更新2 bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                    bulkService.createBulkBatch(updateJobId,list);
                    index++;
                }
            }
            log.info("结束！");

        }

    }

    public void blockHandle1(){

        //Long updateTime = DateUtil8.getTimeStamp(DateUtil8.getAfterOrPreNowTime(DateUtil8.getNowTime_EN(),"hour",-20l));
        //JSONArray accountOffices = crmAPIs.queryArrayV2("accountOffice__c","id,salesOrg__c,cusSAPCode__c,orderBlock__c,delivBlock__c,billingBlock__c","updatedAt>"+updateTime,null);
        JSONArray syncSettings = crmAPIs.queryArrayV2("syncSetting__c","id,name,accountType__c",null,null);
        if(syncSettings.size()>0){
            List<JSONObject> update1 = new ArrayList<>();
            List<JSONObject> update2 = new ArrayList<>();
            for (Object syncSettingObj:syncSettings){
                JSONObject syncSetting = (JSONObject)syncSettingObj;
                String salesOrg = syncSetting.getString("name");
                JSONArray accountOffices = crmAPIs.queryArrayV2("accountOffice__c","id,salesOrg__c,cusSAPCode__c,orderBlock__c,delivBlock__c,billingBlock__c","salesOrg__c ='"+salesOrg+"'",null);
                if(accountOffices.size()>0){
                    for (Object o:accountOffices){
                        JSONObject accountOffice = (JSONObject) o;
                        String cusSAPCode__c = null!=accountOffice.get("cusSAPCode__c")?accountOffice.getString("cusSAPCode__c"):null;
                        String entityType = syncSetting.getString("accountType__c");
                        String [] a = entityType.split("#");
                        for (String type:a){
                            JSONArray accounts = crmAPIs.queryArrayV2("account","id","sapid__c='"+cusSAPCode__c+"' and cusTypeApi__c ='"+type+"'",null);
                            if(accounts.size()>0){
                                for (Object accountObj:accounts){
                                    JSONObject account = (JSONObject)accountObj;
                                    if(null!=accountOffice.get("orderBlock__c") ||null!=accountOffice.get("delivBlock__c")||null!=accountOffice.get("billingBlock__c")){
                                        account.put("salesViewBlock__c",1);
                                        update2.add(account);
                                    }else {
                                        account.put("salesViewBlock__c",2);
                                        update1.add(account);
                                    }
                                }
                            }

                        }

                    }

                    if(CollectionUtils.isNotEmpty(update1)){
                        List<List<JSONObject>> updateLists = splitList(update1,5000);
                        String updateJobId = bulkService.createBulkJob("update","account");
                        int index = 1;
                        for(List list : updateLists){
                            log.info("更新1 bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                            bulkService.createBulkBatch(updateJobId,list);
                            index++;
                        }
                    }

                    if(CollectionUtils.isNotEmpty(update2)){
                        List<List<JSONObject>> updateLists = splitList(update2,5000);
                        String updateJobId = bulkService.createBulkJob("update","account");
                        int index = 1;
                        for(List list : updateLists){
                            log.info("更新2 bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                            bulkService.createBulkBatch(updateJobId,list);
                            index++;
                        }
                    }
                    log.info("结束！");
                }
            }

        }

    }

    /**
     * 分组 list
     *
     * @param list
     * @param pageSize
     * @param <T>
     * @return
     */
    public   <T> List<List<T>> splitList(List<T> list, int pageSize) {
        List<List<T>> listArray = new ArrayList<List<T>>();
        for (int i = 0; i < list.size(); i += pageSize) {
            int toIndex = i + pageSize > list.size() ? list.size() : i + pageSize;
            listArray.add(list.subList(i, toIndex));
        }
        return listArray;
    }
}
