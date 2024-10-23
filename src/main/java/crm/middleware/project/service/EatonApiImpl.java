package crm.middleware.project.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rkhd.platform.sdk.http.CommonData;
import com.rkhd.platform.sdk.http.CommonHttpClient;
import crm.middleware.project.cfg.CrmPropertiesConfig;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.sdk.http.rest.CommonRestClient;
import crm.middleware.project.util.DateUtil8;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.SocketTimeoutException;
import java.util.*;


@Slf4j
@Component
public class EatonApiImpl {

    @Autowired
    CrmPropertiesConfig crmPropertiesConfig;
    @Autowired
    CrmAPIs crmAPIs;
    @Autowired
    BulkService bulkService;
    CommonRestClient commonRestClient = CommonRestClient.getInstance();


    public static String  token = null;

    public static void main(String[] args) {
        String autho = Base64.getEncoder().encodeToString(("CVxo2gSYDnv6JdIj3i5cgFk4s1MvRoEx:twHUpcjRAKyhIchF").getBytes());
        System.out.println(autho);
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

    public  void delete (){
        JSONArray deletes = crmAPIs.queryArrayV2("MiddlewareLog__c","id","logType__c =2",null);
        if(deletes.size()>0){
            List<JSONObject> orders =  deletes.toJavaList(JSONObject.class);
            List<List<JSONObject>> deleteLists = splitList(orders,5000);
            String deleteJobId = bulkService.createBulkJob("delete","MiddlewareLog__c");
            int index = 1;
            for(List list : deleteLists){
                log.info("bulk处理开始，共"+deleteLists.size()+"批次，正在处理第"+index+"条！");
                bulkService.createBulkBatch(deleteJobId,list);
                index++;
            }
        }
    }
    /**
     * 获取集成环境token
     *
     * @return
     */
    public String getToken() {
        com.rkhd.platform.sdk.http.CommonHttpClient commonHttpClient = CommonHttpClient.instance();
        CommonData commonData = CommonData.newBuilder()
                .callType("POST")
                .callString(crmPropertiesConfig.tokenUrl)
                .header("Authorization","Basic " + Base64.getEncoder().encodeToString((crmPropertiesConfig.user  +":"+ crmPropertiesConfig.pwd).getBytes()))
                .build();
        String res = commonHttpClient.performRequest(commonData);
        JSONObject result = JSONObject.parseObject(res);
        token = result.getString("access_token");
        log.info("token获取完成："+token);
        return result.getString("access_token");
    }

    /**
     * 查询订单
     *
     */
    public String querySalesOrder(String createdOn,int skip,int type) throws Exception{
        JSONObject param = new JSONObject();
        Boolean continueFlag = true;
        while (continueFlag){
            String filter = "(DOC_TYPE eq 'ZOR' or DOC_TYPE eq 'ZRE') and  (CREATEDON eq '"+createdOn+"' or Changed_ON eq '" +createdOn+"') and (SALESORG eq '5851' or SALESORG eq '5856')";

            String CPSOrderFilter = "(DOC_TYPE eq 'ZOR' or DOC_TYPE eq 'ZDR' or DOC_TYPE eq 'ZSC' or DOC_TYPE eq 'ZPRJ' or DOC_TYPE eq 'ZCR' or DOC_TYPE eq 'ZRE' or DOC_TYPE eq 'ZWAR') " +
                    "and  (CREATEDON eq '"+createdOn+"' or Changed_ON eq '" +createdOn+"') " +
                    "and (SALESORG eq '5569' or SALESORG eq '5566' or SALESORG eq '5570')";

            String CPQOrderFilter = "(DOC_TYPE eq 'ZOR' or DOC_TYPE eq 'ZDR' or DOC_TYPE eq 'ZCR' or DOC_TYPE eq 'ZRE') " +
                    "and  (CREATEDON eq '"+createdOn+"' or Changed_ON eq '" +createdOn+"') " +
                    "and (SALESORG eq '5560' or SALESORG eq '5563' or SALESORG eq '5566')";

            if(type==1){
                param.put("filter",filter);
            }else if (type==2){
                param.put("filter",CPSOrderFilter);
            }else if (type==3){
                param.put("filter",CPQOrderFilter);
            }

            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("订单查询请求报文："+param.toJSONString());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("interfaceType__c", 1);
            jsonObject.put("logType__c", 1);
            jsonObject.put("date__c", System.currentTimeMillis());
            jsonObject.put("param__c", param.toJSONString());
            JSONObject result = new JSONObject();
            try {
                result = commonRestClient.bearerGetForEaton(token, crmPropertiesConfig.salesorders,param);
            }catch (SocketTimeoutException e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c", "Read timed out");
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;

            }catch (Exception e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c",e.getMessage());
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;
            }

            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject salesOrder = resultJSON.getJSONObject("A_SalesOrder");
                if(null!=salesOrder){
                    JSONArray salesOrders = salesOrder.getJSONArray("A_SalesOrderType");
                    if(null!=result.get("hasMoreRecords")){
                        log.info("total size:"+salesOrders.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                        continueFlag = result.getBoolean("hasMoreRecords");
                        skip = skip+1000;
                    }else {
                        continueFlag = false;
                    }
                    if(salesOrders.size()>0){
                        log.info("订单"+salesOrders.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> accountSet = new HashSet<>();
                        //Set<String> projectSet = new HashSet<>();
                        Set<String> materialSet = new HashSet<>();
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        Set<String> quoteSet = new HashSet<>();
                        Set<String> snycItemSet = new HashSet<>();
                        Set<String> userSet = new HashSet<>();

                        for(Object o : salesOrders){
                            JSONObject order = (JSONObject) o;
                            String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                            accountSet.add("'"+soldToCode__c+"'");
                            String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                            accountSet.add("'"+shipToCode__c+"'");
                    /*String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                    projectSet.add("'"+projCode__c+"'");*/
                            String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                            accountSet.add("'"+pbCusCode__c+"'");
                            String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                            accountSet.add("'"+endUserCode__c+"'");
                            String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                            materialSet.add("'"+materialCode__c+"'");
                            String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                            orderSet.add("'"+name+"'");
                            String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                            orderDetailSet.add("'"+name+detailName+"'");
                            String quoteName = null!=order.get("YourReference")?order.getString("YourReference"):null;
                            quoteSet.add("'"+quoteName+"'");
                            String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                            snycItemSet.add("'"+salesDistrct__c+"'");
                            String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                            snycItemSet.add("'"+rejectCode__c+"'");

                            String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                            userSet.add("'"+userCode__c+"'");
                        }
                        //log.info("quoteSet:"+quoteSet.toString());
                        Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                        //Map<String,String>projectMapInCrm = crmAPIs.queryMapV2(projectSet,"opportunity","id,opportunityId__c","opportunityId__c","id");
                        Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>quoteMapInCrm = crmAPIs.queryMapV2(quoteSet,"quote","name,quotationEntityRelOpportunity","name","quotationEntityRelOpportunity");
                        Map<String,String>snycItemMapInCrm = crmAPIs.queryMapV2(snycItemSet,"snycItem__c","id,itemKey__c","itemKey__c","id");
                        Map<String,String>userMapInCrm = crmAPIs.queryMapV2(userSet,"user","id,employeeCode","employeeCode","id");
                        //log.info("quoteMapInCrm:"+quoteMapInCrm.toString());
                        List<JSONObject> updateOrder = new ArrayList<>();
                        List<JSONObject> updateOrderDetail = new ArrayList<>();
                        List<JSONObject> createOrderDetail = new ArrayList<>();
                        Set<String> createUnique = new HashSet<>();
                        for(Object o : salesOrders){
                            JSONObject order = (JSONObject) o;
                            JSONObject orderCrm = new JSONObject();
                            //处理头
                            String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                            orderCrm.put("name",name);
                            String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                            orderCrm.put("soldToCode__c",soldToCode__c);
                            orderCrm.put("soldTo__c",null!=accountMapInCrm.get(soldToCode__c)?Long.valueOf(accountMapInCrm.get(soldToCode__c)):null);
                            String soldToName__c = (null!=order.get("SoldTo_NAME1")?order.getString("SoldTo_NAME1")+"/":null)
                                    +(null!=order.get("SoldTo_NAME2")?order.getString("SoldTo_NAME2")+" ":null)
                                    +(null!=order.get("SoldTo_NAME3")?order.getString("SoldTo_NAME3")+" ":null)
                                    +(null!=order.get("SoldTo_NAME4")?order.getString("SoldTo_NAME4")+" ":null);
                            orderCrm.put("soldToName__c",soldToName__c);
                            String currency__c = null!=order.get("DOC_CURRCY")?order.getString("DOC_CURRCY"):null;
                            //orderCrm.put("currency__c",2);
                            orderCrm.put("orderCurrency__c",currency__c);
                            String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                            orderCrm.put("shipToCode__c",shipToCode__c);
                            orderCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                            String shipToName__c = (null!=order.get("ShipTo_NAME1")?order.getString("ShipTo_NAME1")+" ":null)
                                    +(null!=order.get("ShipTo_NAME2")?order.getString("ShipTo_NAME2")+" ":null)
                                    +(null!=order.get("ShipTo_NAME3")?order.getString("ShipTo_NAME3")+" ":null)
                                    +(null!=order.get("ShipTo_NAME4")?order.getString("ShipTo_NAME4")+" ":null);
                            orderCrm.put("shipToName__c",shipToName__c);
                            String orderType__c = null!=order.get("DOC_TYPE")?order.getString("DOC_TYPE"):null;
                            orderCrm.put("orderType__c",orderType__c);
                            String distrChan__c = null!=order.get("DISTR_CHAN")?order.getString("DISTR_CHAN"):null;
                            orderCrm.put("distrChan__c",distrChan__c);
                            String profitCentre__c = null!=order.get("PROFIT_CTR")?order.getString("PROFIT_CTR"):null;
                            orderCrm.put("profitCentre__c",profitCentre__c);
                            String projName__c = null!=order.get("ZZPROJNAME")?order.getString("ZZPROJNAME"):null;
                            orderCrm.put("projName__c",projName__c);
                            String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                            orderCrm.put("projCode__c",projCode__c);
                            //orderCrm.put("project__c",null!=projectMapInCrm.get(projCode__c)?Long.valueOf(projectMapInCrm.get(projCode__c)):null);
                            String salesGroup__c = null!=order.get("SALES_GRP")?order.getString("SALES_GRP"):null;
                            orderCrm.put("salesGroup__c",salesGroup__c);
                            String salesOrg__c = null!=order.get("SALESORG")?order.getString("SALESORG"):null;
                            orderCrm.put("salesOrg__c",salesOrg__c);
                            String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                            orderCrm.put("pbCusCode__c",pbCusCode__c);
                            orderCrm.put("pbCus__c",null!=accountMapInCrm.get(pbCusCode__c)?Long.valueOf(accountMapInCrm.get(pbCusCode__c)):null);
                            String pbCusName__c = (null!=order.get("PanelBuilder_NAME1")?order.getString("PanelBuilder_NAME1")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME2")?order.getString("PanelBuilder_NAME2")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME3")?order.getString("PanelBuilder_NAME3")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME4")?order.getString("PanelBuilder_NAME4")+" ":null);
                            orderCrm.put("pbCusName__c",pbCusName__c);
                            String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                            orderCrm.put("endUserCode__c",endUserCode__c);
                            orderCrm.put("endUser__c",null!=accountMapInCrm.get(endUserCode__c)?Long.valueOf(accountMapInCrm.get(endUserCode__c)):null);
                            String endUserName__c = (null!=order.get("EndUser_NAME1")?order.getString("EndUser_NAME1")+" ":null)
                                    +(null!=order.get("EndUser_NAME2")?order.getString("EndUser_NAME2")+" ":null)
                                    +(null!=order.get("EndUser_NAME3")?order.getString("EndUser_NAME3")+" ":null)
                                    +(null!=order.get("EndUser_NAME4")?order.getString("EndUser_NAME4")+" ":null);
                            orderCrm.put("endUserName__c",endUserName__c);
                            String salesOffice__c = null!=order.get("SALES_OFF")?order.getString("SALES_OFF"):null;
                            orderCrm.put("salesOffice__c",salesOffice__c);
                            Long orderDate__c = null!=order.get("DocDate")? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("DocDate")):null;
                            orderCrm.put("orderDate__c",orderDate__c);
                            String itemCateg__c = null!=order.get("ITEM_CATEG")?order.getString("ITEM_CATEG"):null;
                            orderCrm.put("itemCateg__c",itemCateg__c);

                            String salesCreditEmp3__c = (null!=order.get("SalesCreditEmp3_NAME1")?order.getString("SalesCreditEmp3_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME2")?order.getString("SalesCreditEmp3_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME3")?order.getString("SalesCreditEmp3_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME4")?order.getString("SalesCreditEmp3_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp3__c",salesCreditEmp3__c);
                            String salesCreditEmp4__c = (null!=order.get("SalesCreditEmp4_NAME1")?order.getString("SalesCreditEmp4_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME2")?order.getString("SalesCreditEmp4_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME3")?order.getString("SalesCreditEmp4_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME4")?order.getString("SalesCreditEmp4_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp4__c",salesCreditEmp4__c);
                            String salesEmployee__c = (null!=order.get("SalesEmployee_NAME1")?order.getString("SalesEmployee_NAME1")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME2")?order.getString("SalesEmployee_NAME2")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME3")?order.getString("SalesEmployee_NAME3")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4")+" ":null);
                            orderCrm.put("salesEmployee__c",salesEmployee__c);
                            String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                            if(null!=order.get("SalesEmployee_NAME4")&&null!=userMapInCrm.get(userCode__c)){
                                orderCrm.put("ownerId",null!=userMapInCrm.get(userCode__c)?Long.valueOf(userMapInCrm.get(userCode__c)):null);
                            }

                            String salesCreditEmp1__c = (null!=order.get("SalesCreditEmp1_NAME1")?order.getString("SalesCreditEmp1_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME2")?order.getString("SalesCreditEmp1_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME3")?order.getString("SalesCreditEmp1_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME4")?order.getString("SalesCreditEmp1_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp1__c",salesCreditEmp1__c);
                            String salesCreditEmp2__c = (null!=order.get("SalesCreditEmp2_NAME1")?order.getString("SalesCreditEmp2_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME2")?order.getString("SalesCreditEmp2_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME3")?order.getString("SalesCreditEmp2_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME4")?order.getString("SalesCreditEmp2_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp2__c",salesCreditEmp2__c);

                            String salesCrEmp1__c = null!=order.get("SalesCreditEmp1")?order.getString("SalesCreditEmp1"):null;
                            orderCrm.put("salesCrEmp1__c",salesCrEmp1__c);
                            String salesCrEmp2__c = null!=order.get("SalesCreditEmp2")?order.getString("SalesCreditEmp2"):null;
                            orderCrm.put("salesCrEmp2__c",salesCrEmp2__c);
                            String salesCrEmp3__c = null!=order.get("SalesCreditEmp3")?order.getString("SalesCreditEmp3"):null;
                            orderCrm.put("salesCrEmp3__c",salesCrEmp3__c);
                            String salesCrEmp4__c = null!=order.get("SalesCreditEmp4")?order.getString("SalesCreditEmp4"):null;
                            orderCrm.put("salesCrEmp4__c",salesCrEmp4__c);
                            String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                            orderCrm.put("reference__c",reference__c);
                            orderCrm.put("project__c",null!=quoteMapInCrm.get(reference__c)?Long.valueOf(quoteMapInCrm.get(reference__c)):null);
                            String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                            orderCrm.put("salesDistrct__c",null!=snycItemMapInCrm.get(salesDistrct__c)?Long.valueOf(snycItemMapInCrm.get(salesDistrct__c)):null);
                            String purcNo__c  = null!=order.get("PurchaseOrderNo")?order.getString("PurchaseOrderNo"):null;
                            orderCrm.put("purcNo__c",purcNo__c);
                            Long orderId = null;
                            if(null!=orderMapInCrm.get(name)) {
                                orderCrm.put("id",Long.valueOf(orderMapInCrm.get(name)));
                                orderId = Long.valueOf(orderMapInCrm.get(name));
                                updateOrder.add(orderCrm);
                            }else {
                                JSONObject oldOrder = crmAPIs.queryByXoqlApi("select id from salesOrder__c where name = '"+name+"'");
                                if("200".equals(oldOrder.get("code"))){
                                    JSONObject data = oldOrder.getJSONObject("data");
                                    JSONArray records = data.getJSONArray("records");
                                    if(records.size()==0){
                                        orderCrm.put("entityType",crmPropertiesConfig.orderEntityType);
                                        JSONObject salesOrder__c= crmAPIs.createV2X("salesOrder__c",orderCrm);
                                        if("200".equals(salesOrder__c.get("code"))){
                                            JSONObject dataJson = salesOrder__c.getJSONObject("data");
                                            orderId = dataJson.getLong("id");
                                            orderMapInCrm.put(name,orderId.toString());
                                        }
                                    }else {
                                        orderCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                        orderId = ((JSONObject)records.get(0)).getLong("id");
                                        updateOrder.add(orderCrm);
                                        orderMapInCrm.put(name,orderId.toString());
                                    }
                                }
                            }


                            //处理明细
                            JSONObject orderDetailCrm = new JSONObject();
                            String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                            orderDetailCrm.put("name",detailName);
                            String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                            orderDetailCrm.put("materialCode__c",materialCode__c);
                            orderDetailCrm.put("material__c",materialMapInCrm.get(materialCode__c));
                            String materialDesc__c = null!=order.get("MaterialDescription")?order.getString("MaterialDescription"):null;
                            orderDetailCrm.put("materialDesc__c",materialDesc__c);
                            Double qty__c = null!=order.get("CML_OR_QTY")?Double.valueOf(order.getString("CML_OR_QTY")):null;
                            orderDetailCrm.put("qty__c",qty__c);
                            String plantCode__c = null!=order.get("PLANT")?order.getString("PLANT"):null;
                            orderDetailCrm.put("plantCode__c",plantCode__c);
                            Double netPrice__c = null!=order.get("NET_PRICE")?Double.valueOf(order.getString("NET_PRICE")):null;
                            orderDetailCrm.put("netPrice__c",netPrice__c);
                            Double netAmount__c = null!=order.get("NET_VALUE")?Double.valueOf(order.getString("NET_VALUE")):null;
                            orderDetailCrm.put("netAmount__c",netAmount__c);
                            Double taxAmount__c = null!=order.get("TaxAmount")?Double.valueOf(order.getString("TaxAmount")):null;
                            orderDetailCrm.put("taxAmount__c",taxAmount__c);
                            Long firstDate__c = null!=order.get("FirstDate")&&!"".equals(order.get("FirstDate"))? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("FirstDate")):null;
                            orderDetailCrm.put("firstDate__c",firstDate__c);
                            String prodHierarchy__c = null!=order.get("PROD_HIER")?order.getString("PROD_HIER"):null;
                            orderDetailCrm.put("prodHierarchy__c",prodHierarchy__c);
                    /*String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                    orderDetailCrm.put("reference__c",reference__c);*/
                            String subTOTAL_2__c = null!=order.get("SUBTOTAL_2")?order.getString("SUBTOTAL_2"):null;
                            orderDetailCrm.put("subTOTAL_2__c",subTOTAL_2__c);

                            String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                            orderDetailCrm.put("rejectCode__c",null!=snycItemMapInCrm.get(rejectCode__c)?Long.valueOf(snycItemMapInCrm.get(rejectCode__c)):null);

                            orderDetailCrm.put("salesOrder__c",orderId);
                            if(null!=orderDetailMapInCrm.get(name+detailName)){
                                orderDetailCrm.put("id",Long.valueOf(orderDetailMapInCrm.get(name+detailName)));
                                updateOrderDetail.add(orderDetailCrm);
                            }else {

                        /*JSONObject oldOrderDetail = crmAPIs.queryByXoqlApi("select id from salesOrderDetail__c where name = '"+detailName+"'"+" and salesOrder__c = "+orderId);
                        if("200".equals(oldOrderDetail.get("code"))){
                            JSONObject data = oldOrderDetail.getJSONObject("data");
                            JSONArray records = data.getJSONArray("records");
                            if(records.size()==0){
                                orderDetailCrm.put("entityType",crmPropertiesConfig.orderDetailEntityType);
                                JSONObject salesOrderDetail__c= crmAPIs.createV2X("salesOrderDetail__c",orderDetailCrm);
                                if("200".equals(salesOrderDetail__c.get("code"))){
                                    JSONObject dataJson = salesOrderDetail__c.getJSONObject("data");
                                    orderDetailMapInCrm.put(name+detailName,String.valueOf(dataJson.get("id")));
                                }
                            }else {
                                orderDetailCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                updateOrderDetail.add(orderDetailCrm);
                                Long orderDetailId = ((JSONObject)records.get(0)).getLong("id");
                                orderDetailMapInCrm.put(name+detailName,orderDetailId.toString());
                            }
                        }*/
                                orderDetailCrm.put("entityType",crmPropertiesConfig.orderDetailEntityType);
                                if(!createUnique.contains(name+detailName)){
                                    createOrderDetail.add(orderDetailCrm);
                                    createUnique.add(name+detailName);
                                }
                            }

                        }
                        String jobId2 = bulkService.createBulkJob("insert","salesOrderDetail__c");
                        bulkService.createBulkBatch(jobId2,createOrderDetail);
                        String jobId3 = bulkService.createBulkJob("update","salesOrder__c");
                        bulkService.createBulkBatch(jobId3,updateOrder);
                        String jobId4 = bulkService.createBulkJob("update","salesOrderDetail__c");
                        bulkService.createBulkBatch(jobId4,updateOrderDetail);
                    }
                    log.info("订单"+salesOrders.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }
            jsonObject.put("returnMessage__c", "处理成功");
            JSONObject r = crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
            log.info("中间件日志创建结果："+r.toJSONString());
        }

        return null;
    }

    /**
     * 查询销售发票
     *
     */
    public String queryBilling(String createdOn,int skip,int type) throws Exception {
        JSONObject param = new JSONObject();
        //String token = getToken();
        Boolean continueFlag = true;
        while (continueFlag){

            String filter = "(SalesOrg eq '5851'  or SalesOrg eq '5856') and (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn
                    + "') and (BillingType eq 'ZF2' or BillingType eq 'ZF3' or BillingType eq 'ZRE' or BillingType eq 'ZIVS' " +
                    "or BillingType eq 'ZIGS' or BillingType eq 'ZS1' or BillingType eq 'ZS2' or BillingType eq 'ZS3')";
            String CPSBillingFilter = "(SalesOrg eq '5569' or SalesOrg eq '5566' or SalesOrg eq '5570') and (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+ "') " +
                    "and (BillingType eq 'ZL2' or BillingType eq 'ZG2' or BillingType eq 'ZRE' or BillingType eq 'ZFS' " +
                    "or BillingType eq 'ZF2' or BillingType eq 'ZF3' or BillingType eq 'ZIVS' or BillingType eq 'ZIGS' or BillingType eq 'ZS1' or BillingType eq 'ZS2' or BillingType eq 'ZS3')";

            String CPQBillingFilter = "(SalesOrg eq '5560' or SalesOrg eq '5563' or SalesOrg eq '5566') and (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+ "') " +
                    "and (BillingType eq 'ZF2' or BillingType eq 'ZFE' or BillingType eq 'ZG2' or BillingType eq 'ZL2' " +
                    "or BillingType eq 'ZRE' or BillingType eq 'ZS1' or BillingType eq 'ZS2')";

            if(type==1){
                param.put("filter",filter);
            }else if (type==2){
                param.put("filter",CPSBillingFilter);
            }else if (type==3){
                param.put("filter",CPQBillingFilter);
            }
            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("销售发票查询请求报文：" + param.toJSONString());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("interfaceType__c", 3);
            jsonObject.put("logType__c", 1);
            jsonObject.put("date__c", System.currentTimeMillis());
            jsonObject.put("param__c", param.toJSONString());
            JSONObject result = new JSONObject();
            try {
                result = commonRestClient.bearerGetForEaton(token, crmPropertiesConfig.billing, param);
            }catch (SocketTimeoutException e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c", "Read timed out");
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;

            }catch (Exception e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c",e.getMessage());
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;
            }
            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject billingJson = resultJSON.getJSONObject("A_Billing");
                if(null!=billingJson){
                    JSONArray billings = billingJson.getJSONArray("A_BillingType");
                    if(null!=result.get("hasMoreRecords")){
                        log.info("total size:"+billings.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                        continueFlag = result.getBoolean("hasMoreRecords");
                        skip = skip+1000;
                    }else {
                        continueFlag = false;
                    }
                    if(billings.size()>0){
                        log.info("销售发票"+billings.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> accountSet = new HashSet<>();
                        Set<String> materialSet = new HashSet<>();
                        Set<String> billingSet = new HashSet<>();
                        Set<String> billingDetailSet = new HashSet<>();
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        Set<String> createUnique = new HashSet<>();
                        for(Object o : billings){
                            JSONObject billing = (JSONObject) o;
                            String soldCode__c = null!=billing.get("SoldTo")?billing.getString("SoldTo"):null;
                            accountSet.add("'"+soldCode__c+"'");
                            String shipToCode__c = null!=billing.get("ShipTo_KUNNR")?billing.getString("ShipTo_KUNNR"):null;
                            accountSet.add("'"+shipToCode__c+"'");
                            String endUserCode__c = null!=billing.get("EndUser_KUNNR")?billing.getString("EndUser_KUNNR"):null;
                            accountSet.add("'"+endUserCode__c+"'");
                            String pbCode__c = null!=billing.get("PanelBuilder")?billing.getString("PanelBuilder"):null;
                            accountSet.add("'"+pbCode__c+"'");
                            String salesOrderCode__c = null!=billing.get("SalesDocument")?billing.getString("SalesDocument"):null;
                            orderSet.add("'"+salesOrderCode__c+"'");
                            String salesOrderDetailCode__c = null!=billing.get("SalesItem")?billing.getString("SalesItem"):null;
                            orderDetailSet.add("'"+salesOrderCode__c+salesOrderDetailCode__c+"'");
                            String materialCode__c = null!=billing.get("Material")?billing.getString("Material"):null;
                            materialSet.add("'"+materialCode__c+"'");
                            String name = null!=billing.get("BillingDocNo")?billing.getString("BillingDocNo"):null;
                            billingSet.add("'"+name+"'");
                            String detailName = null!=billing.get("ItemNo")?billing.getString("ItemNo"):null;
                            billingDetailSet.add("'"+name+detailName+"'");
                        }
                        Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                        Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>billingMapInCrm = crmAPIs.queryMapV2(billingSet,"billingSAP__c","id,name","name","id");
                        Map<String,String>billingDetailMapInCrm = crmAPIs.queryMapV2(billingDetailSet,"billingDetail__c","id,uniqueKey__c","uniqueKey__c","id");

                        List<JSONObject> updateBilling = new ArrayList<>();
                        List<JSONObject> updateBillingDetail = new ArrayList<>();
                        List<JSONObject> createBillingDetail = new ArrayList<>();
                        for(Object o : billings){
                            JSONObject billing = (JSONObject) o;
                            JSONObject billingCrm = new JSONObject();
                            //处理头
                            String name = null!=billing.get("BillingDocNo")?billing.getString("BillingDocNo"):null;
                            billingCrm.put("name",name);
                            String salesDistrict__c = null!=billing.get("SalesDistrict")?billing.getString("SalesDistrict"):null;
                            billingCrm.put("salesDistrict__c",salesDistrict__c);
                            String salesOrg__c = null!=billing.get("SalesOrg")?billing.getString("SalesOrg"):null;
                            billingCrm.put("salesOrg__c",salesOrg__c);
                            String salesGroup__c = null!=billing.get("SalesGroup")?billing.getString("SalesGroup"):null;
                            billingCrm.put("salesGroup__c",salesGroup__c);
                            String currency__c = null!=billing.get("Currency")?billing.getString("Currency"):null;
                            //billingCrm.put("currency__c",1);
                            billingCrm.put("orderCurrency__c",currency__c);
                            String projCode__c = null!=billing.get("ProjectNo")?billing.getString("ProjectNo"):null;
                            billingCrm.put("projCode__c",projCode__c);
                            String projName__c = null!=billing.get("ProjectName")?billing.getString("ProjectName"):null;
                            billingCrm.put("projName__c",projName__c);
                            String distrChannel__c = null!=billing.get("DistributionChannel")?billing.getString("DistributionChannel"):null;
                            billingCrm.put("distrChannel__c",distrChannel__c);
                            Long billingDate__c = null!=billing.get("BillingDate")? DateUtil8.getTimeStamp_YYYYMMDD(billing.getString("BillingDate")):null;
                            billingCrm.put("billingDate__c",billingDate__c);
                            String billingType__c = null!=billing.get("BillingType")?billing.getString("BillingType"):null;
                            billingCrm.put("billingType__c",billingType__c);
                            String shipToCode__c = null!=billing.get("ShipTo_KUNNR")?billing.getString("ShipTo_KUNNR"):null;
                            billingCrm.put("shipToCode__c",shipToCode__c);
                            billingCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                            String shipToName__c = (null!=billing.get("ShipTo_NAME1")?billing.getString("ShipTo_NAME1")+" ":null)
                                    +(null!=billing.get("ShipTo_NAME2")?billing.getString("ShipTo_NAME2")+" ":null)
                                    +(null!=billing.get("ShipTo_NAME3")?billing.getString("ShipTo_NAME3")+" ":null)
                                    +(null!=billing.get("ShipTo_NAME4")?billing.getString("ShipTo_NAME4")+" ":null);
                            billingCrm.put("shipToName__c",shipToName__c);

                            String soldCode__c = null!=billing.get("SoldTo")?billing.getString("SoldTo"):null;
                            billingCrm.put("soldCode__c",soldCode__c);
                            billingCrm.put("soldTo__c",null!=accountMapInCrm.get(soldCode__c)?Long.valueOf(accountMapInCrm.get(soldCode__c)):null);
                            String soldToName__c = (null!=billing.get("SoldTo_NAME1")?billing.getString("SoldTo_NAME1")+"/":null)
                                    +(null!=billing.get("SoldTo_NAME2")?billing.getString("SoldTo_NAME2")+" ":null)
                                    +(null!=billing.get("SoldTo_NAME3")?billing.getString("SoldTo_NAME3")+" ":null)
                                    +(null!=billing.get("SoldTo_NAME4")?billing.getString("SoldTo_NAME4")+" ":null);
                            billingCrm.put("soldToName__c",soldToName__c);

                            String endUserCode__c = null!=billing.get("EndUser_KUNNR")?billing.getString("EndUser_KUNNR"):null;
                            billingCrm.put("endUserCode__c",endUserCode__c);
                            billingCrm.put("endUser__c",null!=accountMapInCrm.get(endUserCode__c)?Long.valueOf(accountMapInCrm.get(endUserCode__c)):null);
                            String endUserName__c = (null!=billing.get("EndUser_NAME1")?billing.getString("EndUser_NAME1")+" ":null)
                                    +(null!=billing.get("EndUser_NAME2")?billing.getString("EndUser_NAME2")+" ":null)
                                    +(null!=billing.get("EndUser_NAME3")?billing.getString("EndUser_NAME3")+" ":null)
                                    +(null!=billing.get("EndUser_NAME4")?billing.getString("EndUser_NAME4")+" ":null);
                            billingCrm.put("endUserName__c",endUserName__c);

                            String salesOffice__c = null!=billing.get("SalesOffice")?billing.getString("SalesOffice"):null;
                            billingCrm.put("salesOffice__c",salesOffice__c);
                            String projType__c = null!=billing.get("ItemCategory")?billing.getString("ItemCategory"):null;
                            billingCrm.put("projType__c",projType__c);
                            String pbCode__c = null!=billing.get("PanelBuilder")?billing.getString("PanelBuilder"):null;
                            billingCrm.put("pbCode__c",pbCode__c);
                            billingCrm.put("pb__c",null!=accountMapInCrm.get(pbCode__c)?Long.valueOf(accountMapInCrm.get(pbCode__c)):null);
                            String pbName__c = (null!=billing.get("PanelBuilder_NAME1")?billing.getString("PanelBuilder_NAME1")+" ":null)
                                    +(null!=billing.get("PanelBuilder_NAME2")?billing.getString("PanelBuilder_NAME2")+" ":null)
                                    +(null!=billing.get("PanelBuilder_NAME3")?billing.getString("PanelBuilder_NAME3")+" ":null)
                                    +(null!=billing.get("PanelBuilder_NAME4")?billing.getString("PanelBuilder_NAME4")+" ":null);
                            billingCrm.put("pbName__c",pbName__c);

                            String incoterm1__c = null!=billing.get("Incoterm1")?billing.getString("Incoterm1"):null;
                            billingCrm.put("incoterm1__c",incoterm1__c);
                            String incoterm2__c = null!=billing.get("Incoterm2")?billing.getString("Incoterm2"):null;
                            billingCrm.put("incoterm2__c",incoterm2__c);
                            String paymentTerm__c = null!=billing.get("PaymentTerm")?billing.getString("PaymentTerm"):null;
                            billingCrm.put("paymentTerm__c",paymentTerm__c);
                            String profitCenter__c = null!=billing.get("ProfitCenter")?billing.getString("ProfitCenter"):null;
                            billingCrm.put("profitCenter__c",profitCenter__c);

                            Long billingId = null;

                            if(null!=billingMapInCrm.get(name)) {
                                billingCrm.put("id",Long.valueOf(billingMapInCrm.get(name)));
                                billingId = Long.valueOf(billingMapInCrm.get(name));
                                updateBilling.add(billingCrm);
                            }else {
                                JSONObject oldBilling = crmAPIs.queryByXoqlApi("select id from billingSAP__c where name = '"+name+"'");
                                if("200".equals(oldBilling.get("code"))){
                                    JSONObject data = oldBilling.getJSONObject("data");
                                    JSONArray records = data.getJSONArray("records");
                                    if(records.size()==0){
                                        billingCrm.put("entityType",crmPropertiesConfig.billingEntityType);
                                        JSONObject billing__c= crmAPIs.createV2X("billingSAP__c",billingCrm);
                                        if("200".equals(billing__c.get("code"))){
                                            JSONObject dataJson = billing__c.getJSONObject("data");
                                            billingId = dataJson.getLong("id");
                                            billingMapInCrm.put(name,billingId.toString());
                                        }
                                    }else {
                                        billingCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                        billingId = ((JSONObject)records.get(0)).getLong("id");
                                        updateBilling.add(billingCrm);
                                        billingMapInCrm.put(name,billingId.toString());
                                    }
                                }
                            }
                            //处理明细
                            JSONObject  billingDetailCrm = new JSONObject();
                            String detailName = null!=billing.get("ItemNo")?billing.getString("ItemNo"):null;
                            billingDetailCrm.put("name",detailName);
                            String materialCode__c = null!=billing.get("Material")?billing.getString("Material"):null;
                            billingDetailCrm.put("materialCode__c",materialCode__c);
                            billingDetailCrm.put("material__c",null!=materialMapInCrm.get(materialCode__c)?Long.valueOf(materialMapInCrm.get(materialCode__c)):null);
                            String materialDesc__c = null!=billing.get("MaterialDesc")?billing.getString("MaterialDesc"):null;
                            billingDetailCrm.put("materialDesc__c",materialDesc__c);
                            String plant__c = null!=billing.get("Plant")?billing.getString("Plant"):null;
                            billingDetailCrm.put("plant__c",plant__c);
                            Double netValue__c = null!=billing.get("NetValue")?Double.valueOf(billing.getString("NetValue")):null;
                            billingDetailCrm.put("netValue__c",netValue__c);
                            Double taxAmount__c = null!=billing.get("TaxAmount")?Double.valueOf(billing.getString("TaxAmount")):null;
                            billingDetailCrm.put("taxAmount__c",taxAmount__c);
                            Double billingQty__c = null!=billing.get("BilledQty")?Double.valueOf(billing.getString("BilledQty")):null;
                            billingDetailCrm.put("billingQty__c",billingQty__c);

                            String salesOrderCode__c = null!=billing.get("SalesDocument")?billing.getString("SalesDocument"):null;
                            billingDetailCrm.put("salesOrderCode__c",salesOrderCode__c);
                            String salesOrderDetailCode__c = null!=billing.get("SalesItem")?billing.getString("SalesItem"):null;
                            billingDetailCrm.put("salesOrderDetailCode__c",salesOrderDetailCode__c);
                            billingDetailCrm.put("salesOrder__c",null!=orderMapInCrm.get(salesOrderCode__c)?Long.valueOf(orderMapInCrm.get(salesOrderCode__c)):null);
                            billingDetailCrm.put("salesOrderDetail__c",null!=orderDetailMapInCrm.get(salesOrderCode__c+salesOrderDetailCode__c)?Long.valueOf(orderDetailMapInCrm.get(salesOrderCode__c+salesOrderDetailCode__c)):null);

                            billingDetailCrm.put("billingSAP__c",billingId);
                            if(null!=billingDetailMapInCrm.get(name+detailName)){
                                billingDetailCrm.put("id",Long.valueOf(billingDetailMapInCrm.get(name+detailName)));
                                updateBillingDetail.add(billingDetailCrm);
                            }else {
                                billingDetailCrm.put("entityType",crmPropertiesConfig.billingDetailEntityType);
                                if(!createUnique.contains(name+detailName)){
                                    createBillingDetail.add(billingDetailCrm);
                                    createUnique.add(name+detailName);
                                }
                        /*JSONObject oldBillingDetail = crmAPIs.queryByXoqlApi("select id from billingDetail__c where name = '"+detailName+"'"+" and billingSAP__c = "+billingId);
                        if("200".equals(oldBillingDetail.get("code"))){
                            JSONObject data = oldBillingDetail.getJSONObject("data");
                            JSONArray records = data.getJSONArray("records");
                            if(records.size()==0){
                                billingDetailCrm.put("entityType",crmPropertiesConfig.billingDetailEntityType);
                                JSONObject billingDetail__c= crmAPIs.createV2X("billingDetail__c",billingDetailCrm);
                                if("200".equals(billingDetail__c.get("code"))){
                                    JSONObject dataJson = billingDetail__c.getJSONObject("data");
                                    billingDetailMapInCrm.put(name+detailName,String.valueOf(dataJson.get("id")));
                                }
                            }else {
                                billingDetailCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                updateBillingDetail.add(billingDetailCrm);
                                Long billingDetailId = ((JSONObject)records.get(0)).getLong("id");
                                billingDetailMapInCrm.put(name+detailName,billingDetailId.toString());
                            }
                        }*/
                            }
                        }
                        String jobId2 = bulkService.createBulkJob("insert","billingDetail__c");
                        bulkService.createBulkBatch(jobId2,createBillingDetail);
                        String jobId3 = bulkService.createBulkJob("update","billingSAP__c");
                        bulkService.createBulkBatch(jobId3,updateBilling);
                        String jobId4 = bulkService.createBulkJob("update","billingDetail__c");
                        bulkService.createBulkBatch(jobId4,updateBillingDetail);
                    }
                    log.info("销售发票"+billings.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }

            }else {
                continueFlag = false;
            }
            jsonObject.put("returnMessage__c", "处理成功");
            JSONObject r = crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
            log.info("中间件日志创建结果："+r.toJSONString());
        }

        return null;
    }

    /**
     * 查询交货单
     *
     */
    public String queryDelivery(String createdOn,int skip) throws Exception {
        JSONObject param = new JSONObject();
        //String token = getToken();
        Boolean continueFlag = true;
        while (continueFlag){

            String filter = "(SalesOrg eq '5851' or SalesOrg eq '5856') and (DelType eq 'ZLF' or DelType eq 'ZLR' ) and  (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+"')";
            //String filter = "(SalesOrg  eq  '5566' or SalesOrg  eq '5569' or SalesOrg eq '5570') and DelType eq 'ZLF' and  (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+"')";

            param.put("filter", filter);
            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("交货单查询请求报文：" + param.toJSONString());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("interfaceType__c", 2);
            jsonObject.put("logType__c", 1);
            jsonObject.put("date__c", System.currentTimeMillis());
            jsonObject.put("param__c", param.toJSONString());
            JSONObject result = new JSONObject();
            try {
                result = commonRestClient.bearerGetForEaton(token, crmPropertiesConfig.delivery, param);
            }catch (SocketTimeoutException e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c", "Read timed out");
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;

            }catch (Exception e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c",e.getMessage());
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;
            }
            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject deliveryJson = resultJSON.getJSONObject("A_Delivery");
                if(null!=deliveryJson){
                    JSONArray deliverys = deliveryJson.getJSONArray("A_DeliveryType");
                    if(null!=result.get("hasMoreRecords")){
                        log.info("total size:"+deliverys.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                        continueFlag = result.getBoolean("hasMoreRecords");
                        skip = skip+1000;
                    }else {
                        continueFlag = false;
                    }
                    if(deliverys.size()>0){
                        log.info("交货单"+deliverys.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> accountSet = new HashSet<>();
                        Set<String> materialSet = new HashSet<>();
                        Set<String> deliverySet = new HashSet<>();
                        Set<String> deliveryDetailSet = new HashSet<>();
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        Set<String> createUnique = new HashSet<>();
                        Set<String> userSet = new HashSet<>();
                        for(Object o : deliverys){
                            JSONObject delivery = (JSONObject) o;
                            String soldToCode__c = null!=delivery.get("SoldTo")?delivery.getString("SoldTo"):null;
                            accountSet.add("'"+soldToCode__c+"'");
                            String shipToCode__c = null!=delivery.get("KUNNR_ShipTo")?delivery.getString("KUNNR_ShipTo"):null;
                            accountSet.add("'"+shipToCode__c+"'");
                            String orderCode__c = null!=delivery.get("RefSalesDocNo")?delivery.getString("RefSalesDocNo"):null;
                            String orderItemCode__c = null!=delivery.get("RefSalesItemNo")?delivery.getString("RefSalesItemNo"):null;
                            orderSet.add("'"+orderCode__c+"'");
                            orderDetailSet.add("'"+orderCode__c+orderItemCode__c+"'");
                            String materialCode__c = null!=delivery.get("Material")?delivery.getString("Material"):null;
                            materialSet.add("'"+materialCode__c+"'");
                            String name = null!=delivery.get("DeliveryNo")?delivery.getString("DeliveryNo"):null;
                            deliverySet.add("'"+name+"'");
                            String detailName = null!=delivery.get("ItemNo")?delivery.getString("ItemNo"):null;
                            deliveryDetailSet.add("'"+name+detailName+"'");
                            String userCode__c = null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4"):null;
                            userSet.add("'"+userCode__c+"'");
                        }
                        Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                        Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>deliveryMapInCrm = crmAPIs.queryMapV2(deliverySet,"delivery__c","id,name","name","id");
                        Map<String,String>deliveryDetailMapInCrm = crmAPIs.queryMapV2(deliveryDetailSet,"deliveryDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>userMapInCrm = crmAPIs.queryMapV2(userSet,"user","id,employeeCode","employeeCode","id");

                        List<JSONObject> updateDelivery = new ArrayList<>();
                        List<JSONObject> updateDeliveryDetail = new ArrayList<>();
                        List<JSONObject> createDeliveryDetail = new ArrayList<>();
                        for(Object o : deliverys){
                            JSONObject delivery = (JSONObject) o;
                            JSONObject deliveryCrm = new JSONObject();
                            //处理头
                            String name = null!=delivery.get("DeliveryNo")?delivery.getString("DeliveryNo"):null;
                            deliveryCrm.put("name",name);
                            String soldToCode__c = null!=delivery.get("SoldTo")?delivery.getString("SoldTo"):null;
                            deliveryCrm.put("soldTo__c",null!=accountMapInCrm.get(soldToCode__c)?Long.valueOf(accountMapInCrm.get(soldToCode__c)):null);
                            deliveryCrm.put("soldToCode__c",soldToCode__c);
                            String soldToName__c = (null!=delivery.get("NAME1_SoldTo")?delivery.getString("NAME1_SoldTo")+"/":null)
                                    +(null!=delivery.get("NAME2_SoldTo")?delivery.getString("NAME2_SoldTo")+" ":null)
                                    +(null!=delivery.get("NAME3_SoldTo")?delivery.getString("NAME3_SoldTo")+" ":null)
                                    +(null!=delivery.get("NAME4_SoldTo")?delivery.getString("NAME4_SoldTo")+" ":null);
                            deliveryCrm.put("soldToName__c",soldToName__c);

                            String projName__c = null!=delivery.get("ZZPROJNAME")?delivery.getString("ZZPROJNAME"):null;
                            deliveryCrm.put("projName__c",projName__c);
                            String projType__c = null!=delivery.get("ItemCategory")?delivery.getString("ItemCategory"):null;
                            deliveryCrm.put("projType__c",projType__c);
                            String salesOrg__c = null!=delivery.get("SalesOrg")?delivery.getString("SalesOrg"):null;
                            deliveryCrm.put("salesOrg__c",salesOrg__c);
                            String currency__c = null!=delivery.get("Currency")?delivery.getString("Currency"):null;
                            deliveryCrm.put("currency__c",1);
                            deliveryCrm.put("orderCurrency__c",currency__c);
                            String deliveryType__c = null!=delivery.get("DelType")?delivery.getString("DelType"):null;
                            deliveryCrm.put("deliveryType__c",deliveryType__c);
                            String salesOffice__c = null!=delivery.get("SalesOffice")?delivery.getString("SalesOffice"):null;
                            deliveryCrm.put("salesOffice__c",salesOffice__c);
                            String shipToCode__c = null!=delivery.get("KUNNR_ShipTo")?delivery.getString("KUNNR_ShipTo"):null;
                            deliveryCrm.put("shipToCode__c",shipToCode__c);
                            deliveryCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                            String shipToName__c = (null!=delivery.get("NAME1_ShipTo")?delivery.getString("NAME1_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME2_ShipTo")?delivery.getString("NAME2_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME3_ShipTo")?delivery.getString("NAME3_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME4_ShipTo")?delivery.getString("NAME4_ShipTo")+" ":null);
                            deliveryCrm.put("shipToName__c",shipToName__c);

                            String shipToAddress__c = null!=delivery.get("STREET_ShipTo")?delivery.getString("STREET_ShipTo"):null;
                            deliveryCrm.put("shipToAddress__c",shipToAddress__c);
                            String creditStatus__c = null!=delivery.get("OverallStatusOfCreditChecks")?delivery.getString("OverallStatusOfCreditChecks"):null;
                            deliveryCrm.put("creditStatus__c",creditStatus__c);
                            String salesEmployee__c = null!=delivery.get("KUNNR_SalesEmployee")?delivery.getString("KUNNR_SalesEmployee"):null;
                            deliveryCrm.put("salesEmployee__c",salesEmployee__c);
                            String salesEmpName__c = (null!=delivery.get("SalesEmployee_NAME1")?delivery.getString("SalesEmployee_NAME1")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME2")?delivery.getString("SalesEmployee_NAME2")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME3")?delivery.getString("SalesEmployee_NAME3")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4")+" ":null);
                            deliveryCrm.put("salesEmpName__c",salesEmpName__c);
                            String userCode__c = null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4"):null;
                            if(null!=delivery.get("SalesEmployee_NAME4")&&null!=userMapInCrm.get(userCode__c)){
                                deliveryCrm.put("ownerId",null!=userMapInCrm.get(userCode__c)?Long.valueOf(userMapInCrm.get(userCode__c)):null);
                            }
                            Long deliveryId = null;

                            if(null!=deliveryMapInCrm.get(name)) {
                                deliveryCrm.put("id",Long.valueOf(deliveryMapInCrm.get(name)));
                                deliveryId = Long.valueOf(deliveryMapInCrm.get(name));
                                updateDelivery.add(deliveryCrm);
                            }else {
                                JSONObject oldDelivery = crmAPIs.queryByXoqlApi("select id from delivery__c where name = '"+name+"'");
                                if("200".equals(oldDelivery.get("code"))){
                                    JSONObject data = oldDelivery.getJSONObject("data");
                                    JSONArray records = data.getJSONArray("records");
                                    if(records.size()==0){
                                        deliveryCrm.put("entityType",crmPropertiesConfig.deliveryEntityType);
                                        JSONObject delivery__c= crmAPIs.createV2X("delivery__c",deliveryCrm);
                                        if("200".equals(delivery__c.get("code"))){
                                            JSONObject dataJson = delivery__c.getJSONObject("data");
                                            deliveryId = dataJson.getLong("id");
                                            deliveryMapInCrm.put(name,deliveryId.toString());
                                        }
                                    }else {
                                        deliveryCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                        deliveryId = ((JSONObject)records.get(0)).getLong("id");
                                        updateDelivery.add(deliveryCrm);
                                        deliveryMapInCrm.put(name,deliveryId.toString());
                                    }
                                }
                            }
                            //处理明细
                            JSONObject  deliveryDetailCrm = new JSONObject();
                            String detailName = null!=delivery.get("ItemNo")?delivery.getString("ItemNo"):null;
                            deliveryDetailCrm.put("name",detailName);
                            String materialCode__c = null!=delivery.get("Material")?delivery.getString("Material"):null;
                            deliveryDetailCrm.put("materialCode__c",materialCode__c);
                            deliveryDetailCrm.put("material__c",null!=materialMapInCrm.get(materialCode__c)?Long.valueOf(materialMapInCrm.get(materialCode__c)):null);
                            String materialDesc__c = null!=delivery.get("MaterialDesc")?delivery.getString("MaterialDesc"):null;
                            deliveryDetailCrm.put("materialDesc__c",materialDesc__c);

                            String purOrderNo__c = null!=delivery.get("PurchaseOrderNo")?delivery.getString("PurchaseOrderNo"):null;
                            deliveryDetailCrm.put("purOrderNo__c",purOrderNo__c);
                            String orderItemCode__c = null!=delivery.get("RefSalesItemNo")?delivery.getString("RefSalesItemNo"):null;
                            deliveryDetailCrm.put("orderItemCode__c",orderItemCode__c);
                            String orderCode__c = null!=delivery.get("RefSalesDocNo")?delivery.getString("RefSalesDocNo"):null;
                            deliveryDetailCrm.put("orderCode__c",orderCode__c);
                            deliveryDetailCrm.put("salesOrder__c",null!=orderMapInCrm.get(orderCode__c)?Long.valueOf(orderMapInCrm.get(orderCode__c)):null);
                            deliveryDetailCrm.put("salesOrderDetail__c",null!=orderDetailMapInCrm.get(orderCode__c+orderItemCode__c)?Long.valueOf(orderDetailMapInCrm.get(orderCode__c+orderItemCode__c)):null);

                            Long requireDate__c = null!=delivery.get("FirstDate")? DateUtil8.getTimeStamp_YYYYMMDD(delivery.getString("FirstDate")):null;
                            deliveryDetailCrm.put("requireDate__c",requireDate__c);
                            String movingStatus__c = null!=delivery.get("TotalGoodsMovmentStatus")?delivery.getString("TotalGoodsMovmentStatus"):null;
                            deliveryDetailCrm.put("movingStatus__c",movingStatus__c);
                            Long coverDate__c = null!=delivery.get("ActualGoodsMovementDate")? DateUtil8.getTimeStamp_YYYYMMDD(delivery.getString("ActualGoodsMovementDate")):null;
                            deliveryDetailCrm.put("coverDate__c",coverDate__c);
                            Double deliveryQty__c = null!=delivery.get("DeliveryQuantity")?Double.valueOf(delivery.getString("DeliveryQuantity")):null;
                            deliveryDetailCrm.put("deliveryQty__c",deliveryQty__c);
                            String prodHierarchy__c = null!=delivery.get("ProductHierarchy")?delivery.getString("ProductHierarchy"):null;
                            deliveryDetailCrm.put("prodHierarchy__c",prodHierarchy__c);
                            deliveryDetailCrm.put("delivery__c",deliveryId);

                            if(null!=deliveryDetailMapInCrm.get(name+detailName)){
                                deliveryDetailCrm.put("id",Long.valueOf(deliveryDetailMapInCrm.get(name+detailName)));
                                updateDeliveryDetail.add(deliveryDetailCrm);
                            }else {
                                deliveryDetailCrm.put("entityType",crmPropertiesConfig.deliveryDetailEntityType);
                                if(!createUnique.contains(name+detailName)){
                                    createDeliveryDetail.add(deliveryDetailCrm);
                                    createUnique.add(name+detailName);
                                }
                        /*JSONObject oldDeliveryDetail = crmAPIs.queryByXoqlApi("select id from deliveryDetail__c where name = '"+detailName+"'"+" and delivery__c = "+deliveryId);
                        if("200".equals(oldDeliveryDetail.get("code"))){
                            JSONObject data = oldDeliveryDetail.getJSONObject("data");
                            JSONArray records = data.getJSONArray("records");
                            if(records.size()==0){
                                deliveryDetailCrm.put("entityType",crmPropertiesConfig.deliveryDetailEntityType);
                                JSONObject deliveryDetail__c= crmAPIs.createV2X("deliveryDetail__c",deliveryDetailCrm);
                                if("200".equals(deliveryDetail__c.get("code"))){
                                    JSONObject dataJson = deliveryDetail__c.getJSONObject("data");
                                    deliveryDetailMapInCrm.put(name+detailName,String.valueOf(dataJson.get("id")));
                                }
                            }else {
                                deliveryDetailCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                updateDeliveryDetail.add(deliveryDetailCrm);
                                Long deliveryDetailId = ((JSONObject)records.get(0)).getLong("id");
                                deliveryDetailMapInCrm.put(name+detailName,deliveryDetailId.toString());
                            }
                        }*/
                            }
                        }
                        String jobId1 = bulkService.createBulkJob("update","delivery__c");
                        bulkService.createBulkBatch(jobId1,updateDelivery);
                        String jobId2 = bulkService.createBulkJob("update","deliveryDetail__c");
                        bulkService.createBulkBatch(jobId2,updateDeliveryDetail);
                        String jobId3 = bulkService.createBulkJob("insert","deliveryDetail__c");
                        bulkService.createBulkBatch(jobId3,createDeliveryDetail);
                    }
                    log.info("交货单"+deliverys.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }
            jsonObject.put("returnMessage__c", "处理成功");
            JSONObject r = crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
            log.info("中间件日志创建结果："+r.toJSONString());
        }

        return null;
    }


    /**
     * 查询订单
     *
     */
    public String querySalesOrderByOrderNo(String DOC_NUMBER) throws Exception{
        JSONObject param = new JSONObject();
        Boolean continueFlag = true;
        int skip =0;
        while (continueFlag){
            String filter = " DOC_NUMBER eq '"+DOC_NUMBER+"'";
            param.put("filter",filter);
            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("订单查询请求报文："+param.toJSONString());

            JSONObject result = CommonRestClient.getInstance().bearerGetForEaton(token, crmPropertiesConfig.salesorders,param);
            JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
            JSONObject salesOrder = resultJSON.getJSONObject("A_SalesOrder");
            JSONArray salesOrders = salesOrder.getJSONArray("A_SalesOrderType");
            if(null!=result.get("hasMoreRecords")){
                log.info("total size:"+salesOrders.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("entityType",crmPropertiesConfig.middlewareLogEntityType);
                jsonObject.put("type__c",3);
                jsonObject.put("syncParam__c",result);
                //crmAPIs.createV2X("accountSyncParam__c",jsonObject);
                continueFlag = result.getBoolean("hasMoreRecords");
                skip = skip+1000;
            }else {
                continueFlag = false;
            }
            if(salesOrders.size()>0){
                log.info("订单"+salesOrders.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                Set<String> accountSet = new HashSet<>();
                //Set<String> projectSet = new HashSet<>();
                Set<String> materialSet = new HashSet<>();
                Set<String> orderSet = new HashSet<>();
                Set<String> orderDetailSet = new HashSet<>();
                Set<String> quoteSet = new HashSet<>();
                Set<String> snycItemSet = new HashSet<>();
                Set<String> userSet = new HashSet<>();

                for(Object o : salesOrders){
                    JSONObject order = (JSONObject) o;
                    String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                    accountSet.add("'"+soldToCode__c+"'");
                    String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                    accountSet.add("'"+shipToCode__c+"'");
                    /*String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                    projectSet.add("'"+projCode__c+"'");*/
                    String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                    accountSet.add("'"+pbCusCode__c+"'");
                    String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                    accountSet.add("'"+endUserCode__c+"'");
                    String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                    materialSet.add("'"+materialCode__c+"'");
                    String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                    orderSet.add("'"+name+"'");
                    String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                    orderDetailSet.add("'"+name+detailName+"'");
                    String quoteName = null!=order.get("YourReference")?order.getString("YourReference"):null;
                    quoteSet.add("'"+quoteName+"'");
                    String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                    snycItemSet.add("'"+salesDistrct__c+"'");
                    String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                    snycItemSet.add("'"+rejectCode__c+"'");

                    String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                    userSet.add("'"+userCode__c+"'");
                }
                //log.info("quoteSet:"+quoteSet.toString());
                Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                //Map<String,String>projectMapInCrm = crmAPIs.queryMapV2(projectSet,"opportunity","id,opportunityId__c","opportunityId__c","id");
                Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                Map<String,String>quoteMapInCrm = crmAPIs.queryMapV2(quoteSet,"quote","name,quotationEntityRelOpportunity","name","quotationEntityRelOpportunity");
                Map<String,String>snycItemMapInCrm = crmAPIs.queryMapV2(snycItemSet,"snycItem__c","id,itemKey__c","itemKey__c","id");
                Map<String,String>userMapInCrm = crmAPIs.queryMapV2(userSet,"user","id,employeeCode","employeeCode","id");
                //log.info("quoteMapInCrm:"+quoteMapInCrm.toString());
                List<JSONObject> updateOrder = new ArrayList<>();
                List<JSONObject> updateOrderDetail = new ArrayList<>();
                List<JSONObject> createOrderDetail = new ArrayList<>();
                Set<String> createUnique = new HashSet<>();
                for(Object o : salesOrders){
                    JSONObject order = (JSONObject) o;
                    JSONObject orderCrm = new JSONObject();
                    //处理头
                    String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                    orderCrm.put("name",name);
                    String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                    orderCrm.put("soldToCode__c",soldToCode__c);
                    orderCrm.put("soldTo__c",null!=accountMapInCrm.get(soldToCode__c)?Long.valueOf(accountMapInCrm.get(soldToCode__c)):null);
                    String soldToName__c = (null!=order.get("SoldTo_NAME1")?order.getString("SoldTo_NAME1")+"/":null)
                            +(null!=order.get("SoldTo_NAME2")?order.getString("SoldTo_NAME2")+" ":null)
                            +(null!=order.get("SoldTo_NAME3")?order.getString("SoldTo_NAME3")+" ":null)
                            +(null!=order.get("SoldTo_NAME4")?order.getString("SoldTo_NAME4")+" ":null);
                    orderCrm.put("soldToName__c",soldToName__c);
                    String currency__c = null!=order.get("DOC_CURRCY")?order.getString("DOC_CURRCY"):null;
                    //orderCrm.put("currency__c",2);
                    orderCrm.put("orderCurrency__c",currency__c);
                    String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                    orderCrm.put("shipToCode__c",shipToCode__c);
                    orderCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                    String shipToName__c = (null!=order.get("ShipTo_NAME1")?order.getString("ShipTo_NAME1")+" ":null)
                            +(null!=order.get("ShipTo_NAME2")?order.getString("ShipTo_NAME2")+" ":null)
                            +(null!=order.get("ShipTo_NAME3")?order.getString("ShipTo_NAME3")+" ":null)
                            +(null!=order.get("ShipTo_NAME4")?order.getString("ShipTo_NAME4")+" ":null);
                    orderCrm.put("shipToName__c",shipToName__c);
                    String orderType__c = null!=order.get("DOC_TYPE")?order.getString("DOC_TYPE"):null;
                    orderCrm.put("orderType__c",orderType__c);
                    String distrChan__c = null!=order.get("DISTR_CHAN")?order.getString("DISTR_CHAN"):null;
                    orderCrm.put("distrChan__c",distrChan__c);
                    String profitCentre__c = null!=order.get("PROFIT_CTR")?order.getString("PROFIT_CTR"):null;
                    orderCrm.put("profitCentre__c",profitCentre__c);
                    String projName__c = null!=order.get("ZZPROJNAME")?order.getString("ZZPROJNAME"):null;
                    orderCrm.put("projName__c",projName__c);
                    String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                    orderCrm.put("projCode__c",projCode__c);
                    //orderCrm.put("project__c",null!=projectMapInCrm.get(projCode__c)?Long.valueOf(projectMapInCrm.get(projCode__c)):null);
                    String salesGroup__c = null!=order.get("SALES_GRP")?order.getString("SALES_GRP"):null;
                    orderCrm.put("salesGroup__c",salesGroup__c);
                    String salesOrg__c = null!=order.get("SALESORG")?order.getString("SALESORG"):null;
                    orderCrm.put("salesOrg__c",salesOrg__c);
                    String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                    orderCrm.put("pbCusCode__c",pbCusCode__c);
                    orderCrm.put("pbCus__c",null!=accountMapInCrm.get(pbCusCode__c)?Long.valueOf(accountMapInCrm.get(pbCusCode__c)):null);
                    String pbCusName__c = (null!=order.get("PanelBuilder_NAME1")?order.getString("PanelBuilder_NAME1")+" ":null)
                            +(null!=order.get("PanelBuilder_NAME2")?order.getString("PanelBuilder_NAME2")+" ":null)
                            +(null!=order.get("PanelBuilder_NAME3")?order.getString("PanelBuilder_NAME3")+" ":null)
                            +(null!=order.get("PanelBuilder_NAME4")?order.getString("PanelBuilder_NAME4")+" ":null);
                    orderCrm.put("pbCusName__c",pbCusName__c);
                    String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                    orderCrm.put("endUserCode__c",endUserCode__c);
                    orderCrm.put("endUser__c",null!=accountMapInCrm.get(endUserCode__c)?Long.valueOf(accountMapInCrm.get(endUserCode__c)):null);
                    String endUserName__c = (null!=order.get("EndUser_NAME1")?order.getString("EndUser_NAME1")+" ":null)
                            +(null!=order.get("EndUser_NAME2")?order.getString("EndUser_NAME2")+" ":null)
                            +(null!=order.get("EndUser_NAME3")?order.getString("EndUser_NAME3")+" ":null)
                            +(null!=order.get("EndUser_NAME4")?order.getString("EndUser_NAME4")+" ":null);
                    orderCrm.put("endUserName__c",endUserName__c);
                    String salesOffice__c = null!=order.get("SALES_OFF")?order.getString("SALES_OFF"):null;
                    orderCrm.put("salesOffice__c",salesOffice__c);
                    Long orderDate__c = null!=order.get("DocDate")? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("DocDate")):null;
                    orderCrm.put("orderDate__c",orderDate__c);
                    String itemCateg__c = null!=order.get("ITEM_CATEG")?order.getString("ITEM_CATEG"):null;
                    orderCrm.put("itemCateg__c",itemCateg__c);

                    String salesCreditEmp3__c = (null!=order.get("SalesCreditEmp3_NAME1")?order.getString("SalesCreditEmp3_NAME1")+" ":null)
                            +(null!=order.get("SalesCreditEmp3_NAME2")?order.getString("SalesCreditEmp3_NAME2")+" ":null)
                            +(null!=order.get("SalesCreditEmp3_NAME3")?order.getString("SalesCreditEmp3_NAME3")+" ":null)
                            +(null!=order.get("SalesCreditEmp3_NAME4")?order.getString("SalesCreditEmp3_NAME4")+" ":null);
                    orderCrm.put("salesCreditEmp3__c",salesCreditEmp3__c);
                    String salesCreditEmp4__c = (null!=order.get("SalesCreditEmp4_NAME1")?order.getString("SalesCreditEmp4_NAME1")+" ":null)
                            +(null!=order.get("SalesCreditEmp4_NAME2")?order.getString("SalesCreditEmp4_NAME2")+" ":null)
                            +(null!=order.get("SalesCreditEmp4_NAME3")?order.getString("SalesCreditEmp4_NAME3")+" ":null)
                            +(null!=order.get("SalesCreditEmp4_NAME4")?order.getString("SalesCreditEmp4_NAME4")+" ":null);
                    orderCrm.put("salesCreditEmp4__c",salesCreditEmp4__c);
                    String salesEmployee__c = (null!=order.get("SalesEmployee_NAME1")?order.getString("SalesEmployee_NAME1")+" ":null)
                            +(null!=order.get("SalesEmployee_NAME2")?order.getString("SalesEmployee_NAME2")+" ":null)
                            +(null!=order.get("SalesEmployee_NAME3")?order.getString("SalesEmployee_NAME3")+" ":null)
                            +(null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4")+" ":null);
                    orderCrm.put("salesEmployee__c",salesEmployee__c);
                    String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                    if(null!=order.get("SalesEmployee_NAME4")&&null!=userMapInCrm.get(userCode__c)){
                        orderCrm.put("ownerId",null!=userMapInCrm.get(userCode__c)?Long.valueOf(userMapInCrm.get(userCode__c)):null);
                    }

                    String salesCreditEmp1__c = (null!=order.get("SalesCreditEmp1_NAME1")?order.getString("SalesCreditEmp1_NAME1")+" ":null)
                            +(null!=order.get("SalesCreditEmp1_NAME2")?order.getString("SalesCreditEmp1_NAME2")+" ":null)
                            +(null!=order.get("SalesCreditEmp1_NAME3")?order.getString("SalesCreditEmp1_NAME3")+" ":null)
                            +(null!=order.get("SalesCreditEmp1_NAME4")?order.getString("SalesCreditEmp1_NAME4")+" ":null);
                    orderCrm.put("salesCreditEmp1__c",salesCreditEmp1__c);
                    String salesCreditEmp2__c = (null!=order.get("SalesCreditEmp2_NAME1")?order.getString("SalesCreditEmp2_NAME1")+" ":null)
                            +(null!=order.get("SalesCreditEmp2_NAME2")?order.getString("SalesCreditEmp2_NAME2")+" ":null)
                            +(null!=order.get("SalesCreditEmp2_NAME3")?order.getString("SalesCreditEmp2_NAME3")+" ":null)
                            +(null!=order.get("SalesCreditEmp2_NAME4")?order.getString("SalesCreditEmp2_NAME4")+" ":null);
                    orderCrm.put("salesCreditEmp2__c",salesCreditEmp2__c);

                    String salesCrEmp1__c = null!=order.get("SalesCreditEmp1")?order.getString("SalesCreditEmp1"):null;
                    orderCrm.put("salesCrEmp1__c",salesCrEmp1__c);
                    String salesCrEmp2__c = null!=order.get("SalesCreditEmp2")?order.getString("SalesCreditEmp2"):null;
                    orderCrm.put("salesCrEmp2__c",salesCrEmp2__c);
                    String salesCrEmp3__c = null!=order.get("SalesCreditEmp3")?order.getString("SalesCreditEmp3"):null;
                    orderCrm.put("salesCrEmp3__c",salesCrEmp3__c);
                    String salesCrEmp4__c = null!=order.get("SalesCreditEmp4")?order.getString("SalesCreditEmp4"):null;
                    orderCrm.put("salesCrEmp4__c",salesCrEmp4__c);
                    String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                    orderCrm.put("reference__c",reference__c);
                    orderCrm.put("project__c",null!=quoteMapInCrm.get(reference__c)?Long.valueOf(quoteMapInCrm.get(reference__c)):null);
                    String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                    orderCrm.put("salesDistrct__c",null!=snycItemMapInCrm.get(salesDistrct__c)?Long.valueOf(snycItemMapInCrm.get(salesDistrct__c)):null);
                    String purcNo__c  = null!=order.get("PurchaseOrderNo")?order.getString("PurchaseOrderNo"):null;
                    orderCrm.put("purcNo__c",purcNo__c);
                    Long orderId = null;
                    if(null!=orderMapInCrm.get(name)) {
                        orderCrm.put("id",Long.valueOf(orderMapInCrm.get(name)));
                        orderId = Long.valueOf(orderMapInCrm.get(name));
                        updateOrder.add(orderCrm);
                    }else {
                        JSONObject oldOrder = crmAPIs.queryByXoqlApi("select id from salesOrder__c where name = '"+name+"'");
                        if("200".equals(oldOrder.get("code"))){
                            JSONObject data = oldOrder.getJSONObject("data");
                            JSONArray records = data.getJSONArray("records");
                            if(records.size()==0){
                                orderCrm.put("entityType",crmPropertiesConfig.orderEntityType);
                                JSONObject salesOrder__c= crmAPIs.createV2X("salesOrder__c",orderCrm);
                                if("200".equals(salesOrder__c.get("code"))){
                                    JSONObject dataJson = salesOrder__c.getJSONObject("data");
                                    orderId = dataJson.getLong("id");
                                    orderMapInCrm.put(name,orderId.toString());
                                }
                            }else {
                                orderCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                orderId = ((JSONObject)records.get(0)).getLong("id");
                                updateOrder.add(orderCrm);
                                orderMapInCrm.put(name,orderId.toString());
                            }
                        }
                    }


                    //处理明细
                    JSONObject orderDetailCrm = new JSONObject();
                    String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                    orderDetailCrm.put("name",detailName);
                    String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                    orderDetailCrm.put("materialCode__c",materialCode__c);
                    orderDetailCrm.put("material__c",materialMapInCrm.get(materialCode__c));
                    String materialDesc__c = null!=order.get("MaterialDescription")?order.getString("MaterialDescription"):null;
                    orderDetailCrm.put("materialDesc__c",materialDesc__c);
                    Double qty__c = null!=order.get("CML_OR_QTY")?Double.valueOf(order.getString("CML_OR_QTY")):null;
                    orderDetailCrm.put("qty__c",qty__c);
                    String plantCode__c = null!=order.get("PLANT")?order.getString("PLANT"):null;
                    orderDetailCrm.put("plantCode__c",plantCode__c);
                    Double netPrice__c = null!=order.get("NET_PRICE")?Double.valueOf(order.getString("NET_PRICE")):null;
                    orderDetailCrm.put("netPrice__c",netPrice__c);
                    Double netAmount__c = null!=order.get("NET_VALUE")?Double.valueOf(order.getString("NET_VALUE")):null;
                    orderDetailCrm.put("netAmount__c",netAmount__c);
                    Double taxAmount__c = null!=order.get("TaxAmount")?Double.valueOf(order.getString("TaxAmount")):null;
                    orderDetailCrm.put("taxAmount__c",taxAmount__c);
                    Long firstDate__c = null!=order.get("FirstDate")&&!"".equals(order.get("FirstDate"))? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("FirstDate")):null;
                    orderDetailCrm.put("firstDate__c",firstDate__c);
                    String prodHierarchy__c = null!=order.get("PROD_HIER")?order.getString("PROD_HIER"):null;
                    orderDetailCrm.put("prodHierarchy__c",prodHierarchy__c);
                    /*String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                    orderDetailCrm.put("reference__c",reference__c);*/
                    String subTOTAL_2__c = null!=order.get("SUBTOTAL_2")?order.getString("SUBTOTAL_2"):null;
                    orderDetailCrm.put("subTOTAL_2__c",subTOTAL_2__c);

                    String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                    orderDetailCrm.put("rejectCode__c",null!=snycItemMapInCrm.get(rejectCode__c)?Long.valueOf(snycItemMapInCrm.get(rejectCode__c)):null);

                    orderDetailCrm.put("salesOrder__c",orderId);
                    if(null!=orderDetailMapInCrm.get(name+detailName)){
                        orderDetailCrm.put("id",Long.valueOf(orderDetailMapInCrm.get(name+detailName)));
                        updateOrderDetail.add(orderDetailCrm);
                    }else {

                        /*JSONObject oldOrderDetail = crmAPIs.queryByXoqlApi("select id from salesOrderDetail__c where name = '"+detailName+"'"+" and salesOrder__c = "+orderId);
                        if("200".equals(oldOrderDetail.get("code"))){
                            JSONObject data = oldOrderDetail.getJSONObject("data");
                            JSONArray records = data.getJSONArray("records");
                            if(records.size()==0){
                                orderDetailCrm.put("entityType",crmPropertiesConfig.orderDetailEntityType);
                                JSONObject salesOrderDetail__c= crmAPIs.createV2X("salesOrderDetail__c",orderDetailCrm);
                                if("200".equals(salesOrderDetail__c.get("code"))){
                                    JSONObject dataJson = salesOrderDetail__c.getJSONObject("data");
                                    orderDetailMapInCrm.put(name+detailName,String.valueOf(dataJson.get("id")));
                                }
                            }else {
                                orderDetailCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                updateOrderDetail.add(orderDetailCrm);
                                Long orderDetailId = ((JSONObject)records.get(0)).getLong("id");
                                orderDetailMapInCrm.put(name+detailName,orderDetailId.toString());
                            }
                        }*/
                        orderDetailCrm.put("entityType",crmPropertiesConfig.orderDetailEntityType);
                        if(!createUnique.contains(name+detailName)){
                            createOrderDetail.add(orderDetailCrm);
                            createUnique.add(name+detailName);
                        }
                    }

                }
                String jobId2 = bulkService.createBulkJob("insert","salesOrderDetail__c");
                bulkService.createBulkBatch(jobId2,createOrderDetail);
                String jobId3 = bulkService.createBulkJob("update","salesOrder__c");
                bulkService.createBulkBatch(jobId3,updateOrder);
                String jobId4 = bulkService.createBulkJob("update","salesOrderDetail__c");
                bulkService.createBulkBatch(jobId4,updateOrderDetail);
            }
            log.info("订单"+salesOrders.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
        }

        return null;
    }


    /**
     * 删除订单
     *
     */
    public String deleteOrder(String lesn,String gesn) throws Exception {
        JSONObject param = new JSONObject();
        //String token = getToken();
        Boolean continueFlag = true;
        while (continueFlag){

            String filter = "REQTSN ge '"+gesn+"' and REQTSN le '"+lesn+"' and RECORDMODE eq 'R' and SALESORG eq '5851'";

            param.put("filter", filter);

            log.info("删除订单查询请求报文：" + param.toJSONString());
            JSONObject result = CommonRestClient.getInstance().bearerGetForEaton(token, crmPropertiesConfig.delivery, param);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("type__c", 4);
            jsonObject.put("syncParam__c", result);
            //crmAPIs.createV2X("accountSyncParam__c",jsonObject);
            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject orderJson = resultJSON.getJSONObject("A_SalesOrderDeleted");
                if(null!=orderJson){
                    JSONArray orders = orderJson.getJSONArray("A_SalesOrderDeletedType");
                    if(null!=result.get("hasMoreRecords")){
                        continueFlag = result.getBoolean("hasMoreRecords");

                    }else {
                        continueFlag = false;
                    }
                    if(orders.size()>0){
                        log.info("删除订单"+orders.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        for(Object o : orders){
                            JSONObject order = (JSONObject) o;
                            String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                            orderSet.add("'"+name+"'");
                            String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                            orderDetailSet.add("'"+name+detailName+"'");

                        }
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        List<JSONObject> deleteOrder = new ArrayList<>();
                        List<JSONObject> deleteOrderDetail = new ArrayList<>();
                        for (Map.Entry<String, String> entry : orderMapInCrm.entrySet()) {
                            JSONObject delete = new JSONObject();
                            if(null!=entry.getValue()){
                                delete.put("id", Long.valueOf(entry.getValue()));
                                deleteOrder.add(delete);
                            }
                        }
                        for (Map.Entry<String, String> entry : orderDetailMapInCrm.entrySet()) {
                            JSONObject delete = new JSONObject();
                            if(null!=entry.getValue()){
                                delete.put("id", Long.valueOf(entry.getValue()));
                                deleteOrderDetail.add(delete);
                            }
                        }

                        String jobId1 = bulkService.createBulkJob("delete","salesOrder__c");
                        bulkService.createBulkBatch(jobId1,deleteOrder);
                        String jobId2 = bulkService.createBulkJob("delete","salesOrderDetail__c");
                        bulkService.createBulkBatch(jobId2,deleteOrderDetail);
                    }
                    log.info("删除订单"+orders.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }

        }

        return null;
    }

    /**
     * 删除交货单
     *
     */
    public String deleteDelivery(String lesn,String gesn) throws Exception {
        JSONObject param = new JSONObject();
        //String token = getToken();
        Boolean continueFlag = true;
        while (continueFlag){

            String filter = "REQTSN ge '"+gesn+"' and REQTSN le '"+lesn+"' and RECORDMODE eq 'N'";

            param.put("filter", filter);

            log.info("删除交货单查询请求报文：" + param.toJSONString());
            JSONObject result = CommonRestClient.getInstance().bearerGetForEaton(token, crmPropertiesConfig.delivery, param);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("type__c", 4);
            jsonObject.put("syncParam__c", result);
            //crmAPIs.createV2X("accountSyncParam__c",jsonObject);
            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject deliveryJson = resultJSON.getJSONObject("A_DeliveriesDeleted");
                if(null!=deliveryJson){
                    JSONArray deliverys = deliveryJson.getJSONArray("A_DeliveriesDeletedType");
                    if(null!=result.get("hasMoreRecords")){
                        continueFlag = result.getBoolean("hasMoreRecords");

                    }else {
                        continueFlag = false;
                    }
                    if(deliverys.size()>0){
                        log.info("删除交货单"+deliverys.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> deliverySet = new HashSet<>();
                        Set<String> deliveryDetailSet = new HashSet<>();
                        for(Object o : deliverys){
                            JSONObject delivery = (JSONObject) o;
                            String name = null!=delivery.get("DeliveryNo")?delivery.getString("DeliveryNo"):null;
                            deliverySet.add("'"+name+"'");
                            String detailName = null!=delivery.get("ItemNo")?delivery.getString("ItemNo"):null;
                            deliveryDetailSet.add("'"+name+detailName+"'");

                        }
                        Map<String,String>deliveryMapInCrm = crmAPIs.queryMapV2(deliverySet,"delivery__c","id,name","name","id");
                        Map<String,String>deliveryDetailMapInCrm = crmAPIs.queryMapV2(deliveryDetailSet,"deliveryDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        List<JSONObject> deleteDelivery = new ArrayList<>();
                        List<JSONObject> deleteDeliveryDetail = new ArrayList<>();
                        for (Map.Entry<String, String> entry : deliveryMapInCrm.entrySet()) {
                            JSONObject delete = new JSONObject();
                            if(null!=entry.getValue()){
                                delete.put("id", Long.valueOf(entry.getValue()));
                                deleteDelivery.add(delete);
                            }
                        }
                        for (Map.Entry<String, String> entry : deliveryDetailMapInCrm.entrySet()) {
                            JSONObject delete = new JSONObject();
                            if(null!=entry.getValue()){
                                delete.put("id", Long.valueOf(entry.getValue()));
                                deleteDeliveryDetail.add(delete);
                            }
                        }

                        String jobId1 = bulkService.createBulkJob("delete","delivery__c");
                        bulkService.createBulkBatch(jobId1,deleteDelivery);
                        String jobId2 = bulkService.createBulkJob("delete","deliveryDetail__c");
                        bulkService.createBulkBatch(jobId2,deleteDeliveryDetail);
                    }
                    log.info("删除交货单"+deliverys.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }

        }

        return null;
    }

    public String querySalesOrderEveryHour(String createdOn,int skip) throws Exception{
        JSONObject param = new JSONObject();
        Boolean continueFlag = true;
        while (continueFlag){
            String filter = "(DOC_TYPE eq 'ZOR') and  (CREATEDON eq '"+createdOn+"' or Changed_ON eq '" +createdOn+"') and (SALESORG eq '5272')";
            param.put("filter",filter);
            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("订单查询请求报文："+param.toJSONString());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("interfaceType__c", 1);
            jsonObject.put("logType__c", 1);
            jsonObject.put("date__c", System.currentTimeMillis());
            jsonObject.put("param__c", param.toJSONString());
            JSONObject result = new JSONObject();
            try {
                result = commonRestClient.bearerGetForEaton(token, crmPropertiesConfig.salesorders,param);
            }catch (SocketTimeoutException e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c", "Read timed out");
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;

            }catch (Exception e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c",e.getMessage());
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;
            }

            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject salesOrder = resultJSON.getJSONObject("A_SalesOrder");
                if(null!=salesOrder){
                    JSONArray salesOrders = salesOrder.getJSONArray("A_SalesOrderType");
                    if(null!=result.get("hasMoreRecords")){
                        log.info("total size:"+salesOrders.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                        continueFlag = result.getBoolean("hasMoreRecords");
                        skip = skip+1000;
                    }else {
                        continueFlag = false;
                    }
                    if(salesOrders.size()>0){
                        log.info("订单"+salesOrders.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> accountSet = new HashSet<>();
                        //Set<String> projectSet = new HashSet<>();
                        Set<String> materialSet = new HashSet<>();
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        Set<String> quoteSet = new HashSet<>();
                        Set<String> snycItemSet = new HashSet<>();
                        Set<String> userSet = new HashSet<>();

                        for(Object o : salesOrders){
                            JSONObject order = (JSONObject) o;
                            String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                            accountSet.add("'"+soldToCode__c+"'");
                            String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                            accountSet.add("'"+shipToCode__c+"'");
                    /*String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                    projectSet.add("'"+projCode__c+"'");*/
                            String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                            accountSet.add("'"+pbCusCode__c+"'");
                            String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                            accountSet.add("'"+endUserCode__c+"'");
                            String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                            materialSet.add("'"+materialCode__c+"'");
                            String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                            orderSet.add("'"+name+"'");
                            String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                            orderDetailSet.add("'"+name+detailName+"'");
                            String quoteName = null!=order.get("YourReference")?order.getString("YourReference"):null;
                            quoteSet.add("'"+quoteName+"'");
                            String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                            snycItemSet.add("'"+salesDistrct__c+"'");
                            String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                            snycItemSet.add("'"+rejectCode__c+"'");

                            String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                            userSet.add("'"+userCode__c+"'");
                        }
                        //log.info("quoteSet:"+quoteSet.toString());
                        Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                        //Map<String,String>projectMapInCrm = crmAPIs.queryMapV2(projectSet,"opportunity","id,opportunityId__c","opportunityId__c","id");
                        Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>quoteMapInCrm = crmAPIs.queryMapV2(quoteSet,"quote","name,quotationEntityRelOpportunity","name","quotationEntityRelOpportunity");
                        Map<String,String>snycItemMapInCrm = crmAPIs.queryMapV2(snycItemSet,"snycItem__c","id,itemKey__c","itemKey__c","id");
                        Map<String,String>userMapInCrm = crmAPIs.queryMapV2(userSet,"user","id,employeeCode","employeeCode","id");
                        //log.info("quoteMapInCrm:"+quoteMapInCrm.toString());
                        List<JSONObject> updateOrder = new ArrayList<>();
                        List<JSONObject> updateOrderDetail = new ArrayList<>();
                        List<JSONObject> createOrderDetail = new ArrayList<>();
                        Map<String,JSONObject> updateOrderDetailMap = new HashMap<>();
                        Map<String,JSONObject> createOrderDetailMap = new HashMap<>();
                        for(Object o : salesOrders){
                            JSONObject order = (JSONObject) o;
                            JSONObject orderCrm = new JSONObject();
                            //处理头
                            String name = null!=order.get("DOC_NUMBER")?order.getString("DOC_NUMBER"):null;
                            orderCrm.put("name",name);
                            String soldToCode__c = null!=order.get("SOLD_TO")?order.getString("SOLD_TO"):null;
                            orderCrm.put("soldToCode__c",soldToCode__c);
                            orderCrm.put("soldTo__c",null!=accountMapInCrm.get(soldToCode__c)?Long.valueOf(accountMapInCrm.get(soldToCode__c)):null);
                            String soldToName__c = (null!=order.get("SoldTo_NAME1")?order.getString("SoldTo_NAME1")+"/":null)
                                    +(null!=order.get("SoldTo_NAME2")?order.getString("SoldTo_NAME2")+" ":null)
                                    +(null!=order.get("SoldTo_NAME3")?order.getString("SoldTo_NAME3")+" ":null)
                                    +(null!=order.get("SoldTo_NAME4")?order.getString("SoldTo_NAME4")+" ":null);
                            orderCrm.put("soldToName__c",soldToName__c);
                            String currency__c = null!=order.get("DOC_CURRCY")?order.getString("DOC_CURRCY"):null;
                            //orderCrm.put("currency__c",2);
                            orderCrm.put("orderCurrency__c",currency__c);
                            String shipToCode__c = null!=order.get("ShipTo")?order.getString("ShipTo"):null;
                            orderCrm.put("shipToCode__c",shipToCode__c);
                            orderCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                            String shipToName__c = (null!=order.get("ShipTo_NAME1")?order.getString("ShipTo_NAME1")+" ":null)
                                    +(null!=order.get("ShipTo_NAME2")?order.getString("ShipTo_NAME2")+" ":null)
                                    +(null!=order.get("ShipTo_NAME3")?order.getString("ShipTo_NAME3")+" ":null)
                                    +(null!=order.get("ShipTo_NAME4")?order.getString("ShipTo_NAME4")+" ":null);
                            orderCrm.put("shipToName__c",shipToName__c);
                            String orderType__c = null!=order.get("DOC_TYPE")?order.getString("DOC_TYPE"):null;
                            orderCrm.put("orderType__c",orderType__c);
                            String distrChan__c = null!=order.get("DISTR_CHAN")?order.getString("DISTR_CHAN"):null;
                            orderCrm.put("distrChan__c",distrChan__c);
                            String profitCentre__c = null!=order.get("PROFIT_CTR")?order.getString("PROFIT_CTR"):null;
                            orderCrm.put("profitCentre__c",profitCentre__c);
                            String projName__c = null!=order.get("ZZPROJNAME")?order.getString("ZZPROJNAME"):null;
                            orderCrm.put("projName__c",projName__c);
                            String projCode__c = null!=order.get("ZZPROJNUM")?order.getString("ZZPROJNUM"):null;
                            orderCrm.put("projCode__c",projCode__c);
                            //orderCrm.put("project__c",null!=projectMapInCrm.get(projCode__c)?Long.valueOf(projectMapInCrm.get(projCode__c)):null);
                            String salesGroup__c = null!=order.get("SALES_GRP")?order.getString("SALES_GRP"):null;
                            orderCrm.put("salesGroup__c",salesGroup__c);
                            String salesOrg__c = null!=order.get("SALESORG")?order.getString("SALESORG"):null;
                            orderCrm.put("salesOrg__c",salesOrg__c);
                            String pbCusCode__c = null!=order.get("PanelBuilder_Customer")?order.getString("PanelBuilder_Customer"):null;
                            orderCrm.put("pbCusCode__c",pbCusCode__c);
                            orderCrm.put("pbCus__c",null!=accountMapInCrm.get(pbCusCode__c)?Long.valueOf(accountMapInCrm.get(pbCusCode__c)):null);
                            String pbCusName__c = (null!=order.get("PanelBuilder_NAME1")?order.getString("PanelBuilder_NAME1")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME2")?order.getString("PanelBuilder_NAME2")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME3")?order.getString("PanelBuilder_NAME3")+" ":null)
                                    +(null!=order.get("PanelBuilder_NAME4")?order.getString("PanelBuilder_NAME4")+" ":null);
                            orderCrm.put("pbCusName__c",pbCusName__c);
                            String endUserCode__c = null!=order.get("EndUser")?order.getString("EndUser"):null;
                            orderCrm.put("endUserCode__c",endUserCode__c);
                            orderCrm.put("endUser__c",null!=accountMapInCrm.get(endUserCode__c)?Long.valueOf(accountMapInCrm.get(endUserCode__c)):null);
                            String endUserName__c = (null!=order.get("EndUser_NAME1")?order.getString("EndUser_NAME1")+" ":null)
                                    +(null!=order.get("EndUser_NAME2")?order.getString("EndUser_NAME2")+" ":null)
                                    +(null!=order.get("EndUser_NAME3")?order.getString("EndUser_NAME3")+" ":null)
                                    +(null!=order.get("EndUser_NAME4")?order.getString("EndUser_NAME4")+" ":null);
                            orderCrm.put("endUserName__c",endUserName__c);
                            String salesOffice__c = null!=order.get("SALES_OFF")?order.getString("SALES_OFF"):null;
                            orderCrm.put("salesOffice__c",salesOffice__c);
                            Long orderDate__c = null!=order.get("DocDate")? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("DocDate")):null;
                            orderCrm.put("orderDate__c",orderDate__c);
                            String itemCateg__c = null!=order.get("ITEM_CATEG")?order.getString("ITEM_CATEG"):null;
                            orderCrm.put("itemCateg__c",itemCateg__c);

                            String salesCreditEmp3__c = (null!=order.get("SalesCreditEmp3_NAME1")?order.getString("SalesCreditEmp3_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME2")?order.getString("SalesCreditEmp3_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME3")?order.getString("SalesCreditEmp3_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp3_NAME4")?order.getString("SalesCreditEmp3_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp3__c",salesCreditEmp3__c);
                            String salesCreditEmp4__c = (null!=order.get("SalesCreditEmp4_NAME1")?order.getString("SalesCreditEmp4_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME2")?order.getString("SalesCreditEmp4_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME3")?order.getString("SalesCreditEmp4_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp4_NAME4")?order.getString("SalesCreditEmp4_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp4__c",salesCreditEmp4__c);
                            String salesEmployee__c = (null!=order.get("SalesEmployee_NAME1")?order.getString("SalesEmployee_NAME1")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME2")?order.getString("SalesEmployee_NAME2")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME3")?order.getString("SalesEmployee_NAME3")+" ":null)
                                    +(null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4")+" ":null);
                            orderCrm.put("salesEmployee__c",salesEmployee__c);
                            String userCode__c = null!=order.get("SalesEmployee_NAME4")?order.getString("SalesEmployee_NAME4"):null;
                            if(null!=order.get("SalesEmployee_NAME4")&&null!=userMapInCrm.get(userCode__c)){
                                orderCrm.put("ownerId",null!=userMapInCrm.get(userCode__c)?Long.valueOf(userMapInCrm.get(userCode__c)):null);
                            }

                            String salesCreditEmp1__c = (null!=order.get("SalesCreditEmp1_NAME1")?order.getString("SalesCreditEmp1_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME2")?order.getString("SalesCreditEmp1_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME3")?order.getString("SalesCreditEmp1_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp1_NAME4")?order.getString("SalesCreditEmp1_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp1__c",salesCreditEmp1__c);
                            String salesCreditEmp2__c = (null!=order.get("SalesCreditEmp2_NAME1")?order.getString("SalesCreditEmp2_NAME1")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME2")?order.getString("SalesCreditEmp2_NAME2")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME3")?order.getString("SalesCreditEmp2_NAME3")+" ":null)
                                    +(null!=order.get("SalesCreditEmp2_NAME4")?order.getString("SalesCreditEmp2_NAME4")+" ":null);
                            orderCrm.put("salesCreditEmp2__c",salesCreditEmp2__c);

                            String salesCrEmp1__c = null!=order.get("SalesCreditEmp1")?order.getString("SalesCreditEmp1"):null;
                            orderCrm.put("salesCrEmp1__c",salesCrEmp1__c);
                            String salesCrEmp2__c = null!=order.get("SalesCreditEmp2")?order.getString("SalesCreditEmp2"):null;
                            orderCrm.put("salesCrEmp2__c",salesCrEmp2__c);
                            String salesCrEmp3__c = null!=order.get("SalesCreditEmp3")?order.getString("SalesCreditEmp3"):null;
                            orderCrm.put("salesCrEmp3__c",salesCrEmp3__c);
                            String salesCrEmp4__c = null!=order.get("SalesCreditEmp4")?order.getString("SalesCreditEmp4"):null;
                            orderCrm.put("salesCrEmp4__c",salesCrEmp4__c);
                            String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                            orderCrm.put("reference__c",reference__c);
                            orderCrm.put("project__c",null!=quoteMapInCrm.get(reference__c)?Long.valueOf(quoteMapInCrm.get(reference__c)):null);
                            String salesDistrct__c = null!=order.get("SALES_DISTRCT")?"salesDistrct__c"+order.getString("SALES_DISTRCT"):null;
                            orderCrm.put("salesDistrct__c",null!=snycItemMapInCrm.get(salesDistrct__c)?Long.valueOf(snycItemMapInCrm.get(salesDistrct__c)):null);
                            String purcNo__c  = null!=order.get("PurchaseOrderNo")?order.getString("PurchaseOrderNo"):null;
                            orderCrm.put("purcNo__c",purcNo__c);
                            Long orderId = null;
                            if(null!=orderMapInCrm.get(name)) {
                                orderCrm.put("id",Long.valueOf(orderMapInCrm.get(name)));
                                orderId = Long.valueOf(orderMapInCrm.get(name));
                                updateOrder.add(orderCrm);
                            }else {
                                JSONObject oldOrder = crmAPIs.queryByXoqlApi("select id from salesOrder__c where name = '"+name+"'");
                                if("200".equals(oldOrder.get("code"))){
                                    JSONObject data = oldOrder.getJSONObject("data");
                                    JSONArray records = data.getJSONArray("records");
                                    if(records.size()==0){
                                        orderCrm.put("entityType",crmPropertiesConfig.orderEntityType);
                                        JSONObject salesOrder__c= crmAPIs.createV2X("salesOrder__c",orderCrm);
                                        if("200".equals(salesOrder__c.get("code"))){
                                            JSONObject dataJson = salesOrder__c.getJSONObject("data");
                                            orderId = dataJson.getLong("id");
                                            orderMapInCrm.put(name,orderId.toString());
                                            JSONObject quotation = crmAPIs.queryObjectV2X(order.getLongValue("EXTER_ORDER"), "Quotation__c");
                                            if (quotation != null){
                                                crmAPIs.sendNotice(null,null,"发送成功", Collections.singleton(quotation.getJSONObject("cscPic__c").getString("id")),null,null);
                                            }
                                        }
                                    }else {
                                        orderCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                        orderId = ((JSONObject)records.get(0)).getLong("id");
                                        updateOrder.add(orderCrm);
                                        orderMapInCrm.put(name,orderId.toString());
                                    }
                                }
                            }


                            //处理明细
                            JSONObject orderDetailCrm = new JSONObject();
                            String detailName = null!=order.get("S_ORD_ITEM")?order.getString("S_ORD_ITEM"):null;
                            orderDetailCrm.put("name",detailName);
                            String materialCode__c = null!=order.get("MATERIAL")?order.getString("MATERIAL"):null;
                            orderDetailCrm.put("materialCode__c",materialCode__c);
                            orderDetailCrm.put("material__c",materialMapInCrm.get(materialCode__c));
                            String materialDesc__c = null!=order.get("MaterialDescription")?order.getString("MaterialDescription"):null;
                            orderDetailCrm.put("materialDesc__c",materialDesc__c);
                            Double qty__c = null!=order.get("CML_OR_QTY")?Double.valueOf(order.getString("CML_OR_QTY")):null;
                            orderDetailCrm.put("qty__c",qty__c);
                            String plantCode__c = null!=order.get("PLANT")?order.getString("PLANT"):null;
                            orderDetailCrm.put("plantCode__c",plantCode__c);
                            Double netPrice__c = null!=order.get("NET_PRICE")?Double.valueOf(order.getString("NET_PRICE")):null;
                            orderDetailCrm.put("netPrice__c",netPrice__c);
                            Double netAmount__c = null!=order.get("NET_VALUE")?Double.valueOf(order.getString("NET_VALUE")):null;
                            orderDetailCrm.put("netAmount__c",netAmount__c);
                            Double taxAmount__c = null!=order.get("TaxAmount")?Double.valueOf(order.getString("TaxAmount")):null;
                            orderDetailCrm.put("taxAmount__c",taxAmount__c);
                            Long firstDate__c = null!=order.get("FirstDate")&&!"".equals(order.get("FirstDate"))? DateUtil8.getTimeStamp_YYYYMMDD(order.getString("FirstDate")):null;
                            orderDetailCrm.put("firstDate__c",firstDate__c);
                            String prodHierarchy__c = null!=order.get("PROD_HIER")?order.getString("PROD_HIER"):null;
                            orderDetailCrm.put("prodHierarchy__c",prodHierarchy__c);
                    /*String reference__c = null!=order.get("YourReference")?order.getString("YourReference"):null;
                    orderDetailCrm.put("reference__c",reference__c);*/
                            String subTOTAL_2__c = null!=order.get("SUBTOTAL_2")?order.getString("SUBTOTAL_2"):null;
                            orderDetailCrm.put("subTOTAL_2__c",subTOTAL_2__c);

                            String rejectCode__c = null!=order.get("REASON_REJ")?"rejectCode__c"+order.getString("REASON_REJ"):null;
                            orderDetailCrm.put("rejectCode__c",null!=snycItemMapInCrm.get(rejectCode__c)?Long.valueOf(snycItemMapInCrm.get(rejectCode__c)):null);

                            orderDetailCrm.put("salesOrder__c",orderId);
                            String itemNo__c = null!=order.get("EXTER_ITEM")?order.getString("EXTER_ITEM"):null;
                            String quotation__c = null!=order.get("EXTER_ORDER")?order.getString("EXTER_ORDER"):null;
                            JSONArray objects = crmAPIs.queryArrayV2("id", "OrderDetail__c", "itemNo__c ='" + itemNo__c + "' and quotation__c ='" + quotation__c + "'", null);
                            String orderDetail__c = null!=objects&&objects.size()>0?objects.getJSONObject(0).getJSONObject("result").getJSONArray("records").getJSONObject(0).getLong("id").toString():null;
                            orderDetailCrm.put("orderDetail__c",orderDetail__c);
                            orderDetailCrm.put("quotation__c",quotation__c);
                            if(null!=orderDetailMapInCrm.get(name+detailName)){
                                orderDetailCrm.put("id",Long.valueOf(orderDetailMapInCrm.get(name+detailName)));
                                if (null == updateOrderDetailMap.get(name+detailName)){
                                    updateOrderDetailMap.put(name+detailName,orderDetailCrm);
                                }else {
                                    JSONObject object = updateOrderDetailMap.get(name + detailName);
                                    Long firstDate = object.getLongValue("firstDate__c");
                                    if(null != firstDate__c && firstDate < firstDate__c){
                                        object.put("firstDate__c",firstDate__c);
                                    }
                                }
                            }else {
                                orderDetailCrm.put("entityType",crmPropertiesConfig.orderDetailEntityType);
                                if (null == createOrderDetailMap.get(name+detailName)){
                                    createOrderDetailMap.put(name+detailName,orderDetailCrm);
                                }else {
                                    JSONObject object = createOrderDetailMap.get(name + detailName);
                                    Long firstDate = object.getLongValue("firstDate__c");
                                    if(null != firstDate__c && firstDate < firstDate__c){
                                        object.put("firstDate__c",firstDate__c);
                                    }
                                }
                            }
                        }
                        updateOrderDetailMap.forEach((k,v)-> {
                            JSONObject orderDetail = crmAPIs.queryObjectV2X(v.getLongValue("orderDetail__c"), "OrderDetail__c");
                            if (v.getLongValue("firstDate__c") <= orderDetail.getLongValue("firstDate__c")){
                                v.put("beLate__c",1);
                            }else {
                                v.put("beLate__c",2);
                                JSONObject quotation = crmAPIs.queryObjectV2X(v.getLongValue("quotation__c"), "Quotation__c");
                                if (quotation != null){
                                    crmAPIs.sendNotice(null,null,"发送成功", Collections.singleton(quotation.getJSONObject("cscPic__c").getString("id")),null,null);
                                }
                            }
                            updateOrderDetail.add(v);
                        });

                        createOrderDetailMap.forEach((k,v)-> {
                            JSONObject orderDetail = crmAPIs.queryObjectV2X(v.getLong("orderDetail__c"), "OrderDetail__c");
                            if (v.getLongValue("firstDate__c") <= orderDetail.getLongValue("firstDate__c")){
                                v.put("beLate__c",1);
                            }else {
                                v.put("beLate__c",2);
                                JSONObject quotation = crmAPIs.queryObjectV2X(v.getLongValue("quotation__c"), "Quotation__c");
                                if (quotation != null){
                                    crmAPIs.sendNotice(null,null,"发送成功", Collections.singleton(quotation.getJSONObject("cscPic__c").getString("id")),null,null);
                                }
                            }
                            createOrderDetail.add(v);
                        });
                        String jobId2 = bulkService.createBulkJob("insert","salesOrderDetail__c");
                        bulkService.createBulkBatch(jobId2,createOrderDetail);
                        String jobId3 = bulkService.createBulkJob("update","salesOrder__c");
                        bulkService.createBulkBatch(jobId3,updateOrder);
                        String jobId4 = bulkService.createBulkJob("update","salesOrderDetail__c");
                        bulkService.createBulkBatch(jobId4,updateOrderDetail);
                    }
                    log.info("订单"+salesOrders.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }
            jsonObject.put("returnMessage__c", "处理成功");
            JSONObject r = crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
            log.info("中间件日志创建结果："+r.toJSONString());
        }

        return null;
    }

    public String queryDeliveryEveryHour(String createdOn,int skip) throws Exception {
        JSONObject param = new JSONObject();
        //String token = getToken();
        Boolean continueFlag = true;
        while (continueFlag){

            String filter = "(SalesOrg eq '5272') and (DelType eq 'ZLF') and  (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+"')";
            //String filter = "(SalesOrg  eq  '5566' or SalesOrg  eq '5569' or SalesOrg eq '5570') and DelType eq 'ZLF' and  (CreatedON eq '"+createdOn+"' or ChangedON eq '"+createdOn+"')";

            param.put("filter", filter);
            if(skip !=0){
                param.put("skip",skip);
            }
            log.info("交货单查询请求报文：" + param.toJSONString());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("entityType", crmPropertiesConfig.middlewareLogEntityType);
            jsonObject.put("interfaceType__c", 2);
            jsonObject.put("logType__c", 1);
            jsonObject.put("date__c", System.currentTimeMillis());
            jsonObject.put("param__c", param.toJSONString());
            JSONObject result = new JSONObject();
            try {
                result = commonRestClient.bearerGetForEaton(token, crmPropertiesConfig.delivery, param);
            }catch (SocketTimeoutException e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c", "Read timed out");
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;

            }catch (Exception e){
                jsonObject.put("logType__c", 2);
                jsonObject.put("returnMessage__c",e.getMessage());
                crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
                continue;
            }
            if(null!=result.get("result")){
                JSONObject resultJSON = JSONObject.parseObject(result.get("result").toString());
                JSONObject deliveryJson = resultJSON.getJSONObject("A_Delivery");
                if(null!=deliveryJson){
                    JSONArray deliverys = deliveryJson.getJSONArray("A_DeliveryType");
                    if(null!=result.get("hasMoreRecords")){
                        log.info("total size:"+deliverys.size()+ "  hasMoreRecords:"+result.get("hasMoreRecords")+" skip: "+skip);
                        continueFlag = result.getBoolean("hasMoreRecords");
                        skip = skip+1000;
                    }else {
                        continueFlag = false;
                    }
                    if(deliverys.size()>0){
                        log.info("交货单"+deliverys.size()+"条处理开始时间："+DateUtil8.getNowTime_EN());
                        Set<String> accountSet = new HashSet<>();
                        Set<String> materialSet = new HashSet<>();
                        Set<String> deliverySet = new HashSet<>();
                        Set<String> deliveryDetailSet = new HashSet<>();
                        Set<String> orderSet = new HashSet<>();
                        Set<String> orderDetailSet = new HashSet<>();
                        Set<String> createUnique = new HashSet<>();
                        Set<String> userSet = new HashSet<>();
                        for(Object o : deliverys){
                            JSONObject delivery = (JSONObject) o;
                            String soldToCode__c = null!=delivery.get("SoldTo")?delivery.getString("SoldTo"):null;
                            accountSet.add("'"+soldToCode__c+"'");
                            String shipToCode__c = null!=delivery.get("KUNNR_ShipTo")?delivery.getString("KUNNR_ShipTo"):null;
                            accountSet.add("'"+shipToCode__c+"'");
                            String orderCode__c = null!=delivery.get("RefSalesDocNo")?delivery.getString("RefSalesDocNo"):null;
                            String orderItemCode__c = null!=delivery.get("RefSalesItemNo")?delivery.getString("RefSalesItemNo"):null;
                            orderSet.add("'"+orderCode__c+"'");
                            orderDetailSet.add("'"+orderCode__c+orderItemCode__c+"'");
                            String materialCode__c = null!=delivery.get("Material")?delivery.getString("Material"):null;
                            materialSet.add("'"+materialCode__c+"'");
                            String name = null!=delivery.get("DeliveryNo")?delivery.getString("DeliveryNo"):null;
                            deliverySet.add("'"+name+"'");
                            String detailName = null!=delivery.get("ItemNo")?delivery.getString("ItemNo"):null;
                            deliveryDetailSet.add("'"+name+detailName+"'");
                            String userCode__c = null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4"):null;
                            userSet.add("'"+userCode__c+"'");
                        }
                        Map<String,String>accountMapInCrm = crmAPIs.queryMapV2(accountSet,"account","id,sapid__c","sapid__c","id");
                        Map<String,String>materialMapInCrm = crmAPIs.queryMapV2(materialSet,"product","id,sapMCode__c","sapMCode__c","id");
                        Map<String,String>orderMapInCrm = crmAPIs.queryMapV2(orderSet,"salesOrder__c","id,name","name","id");
                        Map<String,String>orderDetailMapInCrm = crmAPIs.queryMapV2(orderDetailSet,"salesOrderDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>deliveryMapInCrm = crmAPIs.queryMapV2(deliverySet,"delivery__c","id,name","name","id");
                        Map<String,String>deliveryDetailMapInCrm = crmAPIs.queryMapV2(deliveryDetailSet,"deliveryDetail__c","id,uniqueKey__c","uniqueKey__c","id");
                        Map<String,String>userMapInCrm = crmAPIs.queryMapV2(userSet,"user","id,employeeCode","employeeCode","id");

                        List<JSONObject> updateDelivery = new ArrayList<>();
                        List<JSONObject> updateDeliveryDetail = new ArrayList<>();
                        List<JSONObject> createDeliveryDetail = new ArrayList<>();
                        for(Object o : deliverys){
                            JSONObject delivery = (JSONObject) o;
                            JSONObject deliveryCrm = new JSONObject();
                            //处理头
                            String name = null!=delivery.get("DeliveryNo")?delivery.getString("DeliveryNo"):null;
                            deliveryCrm.put("name",name);
                            String soldToCode__c = null!=delivery.get("SoldTo")?delivery.getString("SoldTo"):null;
                            deliveryCrm.put("soldTo__c",null!=accountMapInCrm.get(soldToCode__c)?Long.valueOf(accountMapInCrm.get(soldToCode__c)):null);
                            deliveryCrm.put("soldToCode__c",soldToCode__c);
                            String soldToName__c = (null!=delivery.get("NAME1_SoldTo")?delivery.getString("NAME1_SoldTo")+"/":null)
                                    +(null!=delivery.get("NAME2_SoldTo")?delivery.getString("NAME2_SoldTo")+" ":null)
                                    +(null!=delivery.get("NAME3_SoldTo")?delivery.getString("NAME3_SoldTo")+" ":null)
                                    +(null!=delivery.get("NAME4_SoldTo")?delivery.getString("NAME4_SoldTo")+" ":null);
                            deliveryCrm.put("soldToName__c",soldToName__c);

                            String projName__c = null!=delivery.get("ZZPROJNAME")?delivery.getString("ZZPROJNAME"):null;
                            deliveryCrm.put("projName__c",projName__c);
                            String projType__c = null!=delivery.get("ItemCategory")?delivery.getString("ItemCategory"):null;
                            deliveryCrm.put("projType__c",projType__c);
                            String salesOrg__c = null!=delivery.get("SalesOrg")?delivery.getString("SalesOrg"):null;
                            deliveryCrm.put("salesOrg__c",salesOrg__c);
                            String currency__c = null!=delivery.get("Currency")?delivery.getString("Currency"):null;
                            deliveryCrm.put("currency__c",1);
                            deliveryCrm.put("orderCurrency__c",currency__c);
                            String deliveryType__c = null!=delivery.get("DelType")?delivery.getString("DelType"):null;
                            deliveryCrm.put("deliveryType__c",deliveryType__c);
                            String salesOffice__c = null!=delivery.get("SalesOffice")?delivery.getString("SalesOffice"):null;
                            deliveryCrm.put("salesOffice__c",salesOffice__c);
                            String shipToCode__c = null!=delivery.get("KUNNR_ShipTo")?delivery.getString("KUNNR_ShipTo"):null;
                            deliveryCrm.put("shipToCode__c",shipToCode__c);
                            deliveryCrm.put("shipTo__c",null!=accountMapInCrm.get(shipToCode__c)?Long.valueOf(accountMapInCrm.get(shipToCode__c)):null);
                            String shipToName__c = (null!=delivery.get("NAME1_ShipTo")?delivery.getString("NAME1_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME2_ShipTo")?delivery.getString("NAME2_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME3_ShipTo")?delivery.getString("NAME3_ShipTo")+" ":null)
                                    +(null!=delivery.get("NAME4_ShipTo")?delivery.getString("NAME4_ShipTo")+" ":null);
                            deliveryCrm.put("shipToName__c",shipToName__c);

                            String shipToAddress__c = null!=delivery.get("STREET_ShipTo")?delivery.getString("STREET_ShipTo"):null;
                            deliveryCrm.put("shipToAddress__c",shipToAddress__c);
                            String creditStatus__c = null!=delivery.get("OverallStatusOfCreditChecks")?delivery.getString("OverallStatusOfCreditChecks"):null;
                            deliveryCrm.put("creditStatus__c",creditStatus__c);
                            String salesEmployee__c = null!=delivery.get("KUNNR_SalesEmployee")?delivery.getString("KUNNR_SalesEmployee"):null;
                            deliveryCrm.put("salesEmployee__c",salesEmployee__c);
                            String salesEmpName__c = (null!=delivery.get("SalesEmployee_NAME1")?delivery.getString("SalesEmployee_NAME1")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME2")?delivery.getString("SalesEmployee_NAME2")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME3")?delivery.getString("SalesEmployee_NAME3")+" ":null)
                                    +(null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4")+" ":null);
                            deliveryCrm.put("salesEmpName__c",salesEmpName__c);
                            String userCode__c = null!=delivery.get("SalesEmployee_NAME4")?delivery.getString("SalesEmployee_NAME4"):null;
                            if(null!=delivery.get("SalesEmployee_NAME4")&&null!=userMapInCrm.get(userCode__c)){
                                deliveryCrm.put("ownerId",null!=userMapInCrm.get(userCode__c)?Long.valueOf(userMapInCrm.get(userCode__c)):null);
                            }
                            Long deliveryId = null;

                            if(null!=deliveryMapInCrm.get(name)) {
                                deliveryCrm.put("id",Long.valueOf(deliveryMapInCrm.get(name)));
                                deliveryId = Long.valueOf(deliveryMapInCrm.get(name));
                                updateDelivery.add(deliveryCrm);
                            }else {
                                JSONObject oldDelivery = crmAPIs.queryByXoqlApi("select id from delivery__c where name = '"+name+"'");
                                if("200".equals(oldDelivery.get("code"))){
                                    JSONObject data = oldDelivery.getJSONObject("data");
                                    JSONArray records = data.getJSONArray("records");
                                    if(records.size()==0){
                                        deliveryCrm.put("entityType",crmPropertiesConfig.deliveryEntityType);
                                        JSONObject delivery__c= crmAPIs.createV2X("delivery__c",deliveryCrm);
                                        if("200".equals(delivery__c.get("code"))){
                                            JSONObject dataJson = delivery__c.getJSONObject("data");
                                            deliveryId = dataJson.getLong("id");
                                            deliveryMapInCrm.put(name,deliveryId.toString());
                                        }
                                    }else {
                                        deliveryCrm.put("id",((JSONObject)records.get(0)).getLong("id"));
                                        deliveryId = ((JSONObject)records.get(0)).getLong("id");
                                        updateDelivery.add(deliveryCrm);
                                        deliveryMapInCrm.put(name,deliveryId.toString());
                                    }
                                }
                            }
                            //处理明细
                            JSONObject  deliveryDetailCrm = new JSONObject();
                            String detailName = null!=delivery.get("ItemNo")?delivery.getString("ItemNo"):null;
                            deliveryDetailCrm.put("name",detailName);
                            String materialCode__c = null!=delivery.get("Material")?delivery.getString("Material"):null;
                            deliveryDetailCrm.put("materialCode__c",materialCode__c);
                            deliveryDetailCrm.put("material__c",null!=materialMapInCrm.get(materialCode__c)?Long.valueOf(materialMapInCrm.get(materialCode__c)):null);
                            String materialDesc__c = null!=delivery.get("MaterialDesc")?delivery.getString("MaterialDesc"):null;
                            deliveryDetailCrm.put("materialDesc__c",materialDesc__c);

                            String purOrderNo__c = null!=delivery.get("PurchaseOrderNo")?delivery.getString("PurchaseOrderNo"):null;
                            deliveryDetailCrm.put("purOrderNo__c",purOrderNo__c);
                            String orderItemCode__c = null!=delivery.get("RefSalesItemNo")?delivery.getString("RefSalesItemNo"):null;
                            deliveryDetailCrm.put("orderItemCode__c",orderItemCode__c);
                            String orderCode__c = null!=delivery.get("RefSalesDocNo")?delivery.getString("RefSalesDocNo"):null;
                            deliveryDetailCrm.put("orderCode__c",orderCode__c);
                            deliveryDetailCrm.put("salesOrder__c",null!=orderMapInCrm.get(orderCode__c)?Long.valueOf(orderMapInCrm.get(orderCode__c)):null);
                            deliveryDetailCrm.put("salesOrderDetail__c",null!=orderDetailMapInCrm.get(orderCode__c+orderItemCode__c)?Long.valueOf(orderDetailMapInCrm.get(orderCode__c+orderItemCode__c)):null);

                            Long requireDate__c = null!=delivery.get("FirstDate")? DateUtil8.getTimeStamp_YYYYMMDD(delivery.getString("FirstDate")):null;
                            deliveryDetailCrm.put("requireDate__c",requireDate__c);
                            String movingStatus__c = null!=delivery.get("TotalGoodsMovmentStatus")?delivery.getString("TotalGoodsMovmentStatus"):null;
                            deliveryDetailCrm.put("movingStatus__c",movingStatus__c);
                            Long coverDate__c = null!=delivery.get("ActualGoodsMovementDate")? DateUtil8.getTimeStamp_YYYYMMDD(delivery.getString("ActualGoodsMovementDate")):null;
                            deliveryDetailCrm.put("coverDate__c",coverDate__c);
                            Double deliveryQty__c = null!=delivery.get("DeliveryQuantity")?Double.valueOf(delivery.getString("DeliveryQuantity")):null;
                            deliveryDetailCrm.put("deliveryQty__c",deliveryQty__c);
                            String prodHierarchy__c = null!=delivery.get("ProductHierarchy")?delivery.getString("ProductHierarchy"):null;
                            deliveryDetailCrm.put("prodHierarchy__c",prodHierarchy__c);
                            JSONObject salesOrderDetail = crmAPIs.queryObjectV2X(null!=orderItemCode__c? Long.parseLong(orderItemCode__c) :0, "salesOrderDetail__c");
                            deliveryDetailCrm.put("quotation__c",salesOrderDetail.get("quotation__c"));
                            deliveryDetailCrm.put("delivery__c",deliveryId);

                            if(null!=deliveryDetailMapInCrm.get(name+detailName)){
                                deliveryDetailCrm.put("id",Long.valueOf(deliveryDetailMapInCrm.get(name+detailName)));
                                updateDeliveryDetail.add(deliveryDetailCrm);
                            }else {
                                deliveryDetailCrm.put("entityType",crmPropertiesConfig.deliveryDetailEntityType);
                                if(!createUnique.contains(name+detailName)){
                                    createDeliveryDetail.add(deliveryDetailCrm);
                                    createUnique.add(name+detailName);
                                }
                            }
                        }
                        String jobId1 = bulkService.createBulkJob("update","delivery__c");
                        bulkService.createBulkBatch(jobId1,updateDelivery);
                        String jobId2 = bulkService.createBulkJob("update","deliveryDetail__c");
                        bulkService.createBulkBatch(jobId2,updateDeliveryDetail);
                        String jobId3 = bulkService.createBulkJob("insert","deliveryDetail__c");
                        bulkService.createBulkBatch(jobId3,createDeliveryDetail);
                    }
                    log.info("交货单"+deliverys.size()+"条处理结束时间："+DateUtil8.getNowTime_EN());
                }else {
                    continueFlag = false;
                }
            }else {
                continueFlag = false;
            }
            jsonObject.put("returnMessage__c", "处理成功");
            JSONObject r = crmAPIs.createV2X("MiddlewareLog__c",jsonObject);
            log.info("中间件日志创建结果："+r.toJSONString());
        }

        return null;
    }

}
