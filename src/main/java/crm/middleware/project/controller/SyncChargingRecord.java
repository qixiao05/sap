package crm.middleware.project.controller;

import crm.middleware.project.task.TaskCenter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class SyncChargingRecord {
    @Autowired
    TaskCenter taskCenter;

    @RequestMapping("/Sap/SyncChargingRecordEveryHour")
    public ResponseEntity<String> syncChargingRecord(){
        try {
            taskCenter.syncChargingRecordEveryHour();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

}
