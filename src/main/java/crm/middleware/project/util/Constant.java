package crm.middleware.project.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class Constant {

    /*系统参数匹配通用选项集信息*/
    /*根据code查询通过选项集的选项id*/
    public static Map<String, Map<String, String>> sysParamsIdByCodeMap = new HashMap<>();
    /*根据id查询三方code*/
    public static Map<String, Map<String, String>> sysParamsCodeByIdMap = new HashMap<>();
    /*根据对象（JSON）查询三方code*/
    public static Map<String, Map<String, JSONObject>> sysParamsJsonByIdMap = new HashMap<>();

}
