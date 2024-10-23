package crm.middleware.project.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.cfg.CrmPropertiesConfig;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.util.DateUtil8;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.rmi.server.ExportException;
import java.util.*;


@Slf4j
@Component
public class PredictedShareService {

    @Autowired
    CrmPropertiesConfig crmPropertiesConfig;
    @Autowired
    CrmAPIs crmAPIs;
    @Autowired
    BulkService bulkService;

    public static void main(String[] args) throws Exception {

        String timeStr = DateUtil8.getLastDayOfMonth(DateUtil8.getAfterOrPreDate(DateUtil8.getCurrentMonthFirstDay(),"month",-1),false);

        System.out.println(timeStr);

    }


    public void predictedShare() throws Exception{
        //String timeStr = "2024-04-30";
        String timeStr = DateUtil8.getLastDayOfMonth(DateUtil8.getAfterOrPreDate(DateUtil8.getCurrentMonthFirstDay(),"month",-1),false);
        Long time = DateUtil8.getTimeStamp(timeStr+" 23:59:59");
        List<JSONObject> adds = new ArrayList<>();

        JSONArray oppos = crmAPIs.queryArrayV2("opportunity",
                "id,ownerId,exclusivePLC__c,supplyAmount__c,forcastAmount__c","createdAt <="+time
                        +" and entityType = "+crmPropertiesConfig.oppoOEMEntityType,null);
        log.info("query sie: "+oppos.size());
        Set<Long> users = new HashSet<>();
        Map<Long,Double> exclusivePLCMap = new HashMap<>();
        Map<Long,Double> supplyAmountMap = new HashMap<>();
        Map<Long,Double> forcastAmountMap = new HashMap<>();
        if(oppos.size()>0 ){
            for(Object o:oppos){
                JSONObject oppo = (JSONObject) o;
                Long ownerId = oppo.getLong("ownerId");
                users.add(ownerId);
                exclusivePLCMap.put(ownerId,(null!=exclusivePLCMap.get(ownerId)?exclusivePLCMap.get(ownerId):0)+(null!=oppo.get("exclusivePLC__c")?oppo.getDouble("exclusivePLC__c"):0));
                supplyAmountMap.put(ownerId,(null!=supplyAmountMap.get(ownerId)?supplyAmountMap.get(ownerId):0)+(null!=oppo.get("supplyAmount__c")?oppo.getDouble("supplyAmount__c"):0));
                forcastAmountMap.put(ownerId,(null!=forcastAmountMap.get(ownerId)?forcastAmountMap.get(ownerId):0)+(null!=oppo.get("forcastAmount__c")?oppo.getDouble("forcastAmount__c"):0));
            }
            for(Long userId:users){
                JSONObject add = new JSONObject();
                add.put("entityType",crmPropertiesConfig.predictedDefaultEntityType);
                add.put("user__c",userId);
                add.put("ownerId",userId);
                add.put("opportunity_date__c",time);
                add.put("exclusivePLC__c",exclusivePLCMap.get(userId));
                add.put("forcastAmount__c",supplyAmountMap.get(userId));
                add.put("supplyAmount__c",forcastAmountMap.get(userId));
                adds.add(add);
            }
        }



        if(CollectionUtils.isNotEmpty(adds)){
            List<List<JSONObject>> updateLists = splitList(adds,5000);
            String addJobId = bulkService.createBulkJob("insert","predicted_share__c");
            int index = 1;
            for(List list : updateLists){
                log.info("新增bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                bulkService.createBulkBatch(addJobId,list);
                index++;
            }
        }
    }

    public void predictedShare1() throws Exception{
        JSONArray predicted_shareArray = crmAPIs.queryArrayV2("predicted_share__c",
                "id",null,null);
        if(CollectionUtils.isNotEmpty(predicted_shareArray)){
            List<JSONObject> predicted_shares = new ArrayList<>();
            for(Object o:predicted_shareArray){
                JSONObject predicted_share = (JSONObject) o;
                predicted_shares.add(predicted_share);
            }
            List<List<JSONObject>> deleteLists = splitList(predicted_shares,5000);
            String deleteJobId = bulkService.createBulkJob("delete","predicted_share__c");
            int index = 1;
            for(List list : deleteLists){
                log.info("删除bulk处理开始，共"+deleteLists.size()+"批次，正在处理第"+index+"条！");
                bulkService.createBulkBatch(deleteJobId,list);
                index++;
            }
        }

        JSONArray opportunity = crmAPIs.queryArrayV2("opportunity",
                "id,createdAt","entityType = "+crmPropertiesConfig.oppoOEMEntityType,"order by createdAt asc limit 1");
        Long judgeTime = opportunity.getJSONObject(0).getLong("createdAt");

        String startStr = DateUtil8.getCurrentMonthFirstDay();
        Long start = DateUtil8.getTimeStamp(startStr+" 00:00:00");
        String endStr = DateUtil8.getLastDayOfMonth(DateUtil8.getCurrentMonthFirstDay(),false);
        Long end = DateUtil8.getTimeStamp(endStr+" 23:59:59");
        Boolean continueFlag = true;
        List<JSONObject> adds = new ArrayList<>();
        while (continueFlag){
            if(end.longValue()<=judgeTime.longValue()){
                continueFlag = false;
            }
            JSONArray oppos = crmAPIs.queryArrayV2("opportunity",
                    "id,ownerId,exclusivePLC__c,supplyAmount__c,forcastAmount__c","createdAt>="+start+" and createdAt <="+end
                            +" and entityType = "+crmPropertiesConfig.oppoOEMEntityType,null);
            Set<Long> users = new HashSet<>();
            Map<Long,Double> exclusivePLCMap = new HashMap<>();
            Map<Long,Double> supplyAmountMap = new HashMap<>();
            Map<Long,Double> forcastAmountMap = new HashMap<>();
            if(oppos.size()>0 ){
                for(Object o:oppos){
                    JSONObject oppo = (JSONObject) o;
                    Long ownerId = oppo.getLong("ownerId");
                    users.add(ownerId);
                    exclusivePLCMap.put(ownerId,(null!=exclusivePLCMap.get(ownerId)?exclusivePLCMap.get(ownerId):0)+(null!=oppo.get("exclusivePLC__c")?oppo.getDouble("exclusivePLC__c"):0));
                    supplyAmountMap.put(ownerId,(null!=supplyAmountMap.get(ownerId)?supplyAmountMap.get(ownerId):0)+(null!=oppo.get("supplyAmount__c")?oppo.getDouble("supplyAmount__c"):0));
                    forcastAmountMap.put(ownerId,(null!=forcastAmountMap.get(ownerId)?forcastAmountMap.get(ownerId):0)+(null!=oppo.get("forcastAmount__c")?oppo.getDouble("forcastAmount__c"):0));
                }
                for(Long userId:users){
                    JSONObject add = new JSONObject();
                    add.put("entityType",crmPropertiesConfig.predictedDefaultEntityType);
                    add.put("user__c",userId);
                    add.put("ownerId",userId);
                    add.put("opportunity_date__c",end);
                    add.put("exclusivePLC__c",exclusivePLCMap.get(userId));
                    add.put("forcastAmount__c",supplyAmountMap.get(userId));
                    add.put("supplyAmount__c",forcastAmountMap.get(userId));
                    adds.add(add);
                }
            }

            startStr = DateUtil8.getAfterOrPreDate(startStr,"month",-1);
            start = DateUtil8.getTimeStamp(startStr+" 00:00:00");
            endStr = DateUtil8.getLastDayOfMonth(startStr,false);
            end = DateUtil8.getTimeStamp(endStr+" 23:59:59");
        }

        if(CollectionUtils.isNotEmpty(adds)){
            List<List<JSONObject>> updateLists = splitList(adds,5000);
            String addJobId = bulkService.createBulkJob("insert","predicted_share__c");
            int index = 1;
            for(List list : updateLists){
                log.info("新增bulk处理开始，共"+updateLists.size()+"批次，正在处理第"+index+"条！");
                bulkService.createBulkBatch(addJobId,list);
                index++;
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
