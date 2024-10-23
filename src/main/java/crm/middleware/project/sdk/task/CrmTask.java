package crm.middleware.project.sdk.task;

import crm.middleware.project.sdk.http.config.CrmConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CrmTask {

    @Scheduled(cron = "0 0 4 * * ?")    //每天四点清空
    public void clearMaps() {

        /**
         * 清空消息
         */
        try {
            CrmConfig.notifyMap.clear();
            log.info("清空notifyMap...");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
