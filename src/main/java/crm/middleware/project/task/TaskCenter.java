package crm.middleware.project.task;

import crm.middleware.project.service.*;
import crm.middleware.project.util.DateUtil8;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;


/**
 * @program: com.sqmzk.project
 * @description: 定时任务类
 * @author: Mike
 * @create: 2020-10-28 14:14
 **/
@Slf4j
@Component
public class TaskCenter {

    @Autowired
    EatonApiImpl eatonApi;
    @Autowired
    BlockHandleService blockHandleService;
    @Autowired
    PredictedShareService predictedShareService;
    @Autowired
    BsmProductMatchJob bsmProductMatchJob;
    @Autowired
    MistakeHandleService mistakeHandleService;

    @Scheduled(initialDelay = 2000, fixedRate = 1000 * 60 * 60 * 24)
    public void testTask() {
        log.info("------------项目运行开始-------------");
        try {
            eatonApi.getToken();
            //String date = DateUtil8.getAfterOrPreNowTimePlus(DateUtil8.yyyyMMdd,"day",-1l);
            //eatonApi.querySalesOrderByOrderNo("0225397281");
            //bsmProductMatchJob.match();

            /*String date = "20231101";
            eatonApi.querySalesOrder(date,0,1);
            eatonApi.queryBilling(date,0,1);
            eatonApi.queryDelivery(date,0);*/

            /*long index = -176;
            int finalDate = 20240625;
            int startDate = 20240101;
            while (startDate<finalDate){
                String date = DateUtil8.getAfterOrPreNowTimePlus(DateUtil8.yyyyMMdd,"day",index);
                log.info("开始处理："+date);
                eatonApi.querySalesOrder(date,0,1);
                eatonApi.queryBilling(date,0,1);
                eatonApi.queryDelivery(date,0);
                log.info("处理完成："+date);
                startDate = Integer.valueOf(date);
                index++;
            }*/

            //mistakeHandleService.hadleMissOrder();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //数据同步（每天1：00 ：00）（开）
/*    @Scheduled(initialDelay = 2000, fixedRate = 1000 * 60 * 60 * 24)
    @Async
    @Scheduled(cron = "0 0 1 * * ?")*/
    public void syncChargingRecord() throws Exception{
        //try {
            //String date = DateUtil8.getAfterOrPreNowTime("day",-1l);
        String date = DateUtil8.getAfterOrPreNowTimePlus(DateUtil8.yyyyMMdd,"day",-1l);
        //String date = "20240506";
            log.info("订单处理开始时间："+DateUtil8.getNowTime_EN());
            eatonApi.querySalesOrder(date,0,1);

            /*eatonApi.querySalesOrder(date,0,2);
            eatonApi.querySalesOrder(date,0,3);*/
            log.info("订单处理结束时间："+DateUtil8.getNowTime_EN());
            log.info("销售发票处理开始时间："+DateUtil8.getNowTime_EN());
            eatonApi.queryBilling(date,0,1);

            /*eatonApi.queryBilling(date,0,2);
            eatonApi.queryBilling(date,0,3);*/
            log.info("销售发票处理结束时间："+DateUtil8.getNowTime_EN());
            log.info("交货单处理开始时间："+DateUtil8.getNowTime_EN());
            eatonApi.queryDelivery(date,0);
            log.info("交货单处理结束时间："+DateUtil8.getNowTime_EN());
        /*}catch (Exception e){
            log.error("任务执行出错："+e.getMessage());
        }*/

    }

    //销售组织（SALESORG）为"5272"且订单类型（DOC_TYPE）为”ZOR“的订单信息
    //销售组织（SalesOrg）为"5272"且交货类型（DelType）为”ZLF“的交货单信息。
    //每小时执行一次
    @Scheduled(initialDelay = 2000, fixedRate = 1000 * 60 * 60)
    @Scheduled(cron = "0 0 * * * ?")
    @Async
    public void syncChargingRecordEveryHour() throws Exception{
        String date = DateUtil8.getAfterOrPreNowTimePlus(DateUtil8.yyyyMMdd,"hour",-12l);
        log.info("订单处理开始时间："+DateUtil8.getNowTime_EN());
        eatonApi.querySalesOrderEveryHour(date,0);
        log.info("订单处理结束时间："+DateUtil8.getNowTime_EN());
        log.info("销售发票处理开始时间："+DateUtil8.getNowTime_EN());
        eatonApi.queryBilling(date,0,1);
        log.info("销售发票处理结束时间："+DateUtil8.getNowTime_EN());
        log.info("交货单处理开始时间："+DateUtil8.getNowTime_EN());
        eatonApi.queryDeliveryEveryHour(date,0);
        log.info("交货单处理结束时间："+DateUtil8.getNowTime_EN());
    }

    //get token
    // 每小时30分执行一次
    @Scheduled(cron = "0 30 * * * ?")
    @Async
    public void getToken() {
        try {
            eatonApi.getToken();
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }


    /**
     * blockHandle
     * 每10分执行一次
     */
    @Scheduled(cron = "0 0/10 * * * ?")
    //@Scheduled(cron = "0 0/3 * * * ?")
    @Async
    public void blockHandle() {
        try {
            blockHandleService.blockHandle();
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }
    public static void main(String[] args) {
        long index = -176;
        int finalDate = 20240625;
        int startDate = 20240101;
        while (startDate<finalDate){
            String date2 = DateUtil8.getAfterOrPreNowTimePlus(DateUtil8.yyyyMMdd,"day",index);
            startDate = Integer.valueOf(date2);
            System.out.println(startDate);
            index++;
        }

    }

}
