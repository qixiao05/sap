package crm.middleware.project.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.sdk.CrmAPIs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: crm.dian.project
 * @description:
 * @author: Mike
 * @create: 2020-09-17 14:48
 **/

@Component
@Slf4j
public class ComUtils {

@Autowired
    CrmAPIs crmAPIs;

    /**
     * 分组 list
     *
     * @param list
     * @param pageSize
     * @param <T>
     * @return
     */
    public  static   <T> List<List<T>> splitList(List<T> list, int pageSize) {
        List<List<T>> listArray = new ArrayList<List<T>>();
        for (int i = 0; i < list.size(); i += pageSize) {
            int toIndex = i + pageSize > list.size() ? list.size() : i + pageSize;
            listArray.add(list.subList(i, toIndex));
        }
        return listArray;
    }

    /**
     * 递归查找部门
     *
     * @param dimDepart
     * @param departList
     * @return
     */
    public  List<Long> queryDepart(Long dimDepart, List<Long> departList) {
        JSONArray array = crmAPIs.queryArrayV1("department", " id,parentDepartId,departName ,departCode ", "id=" + dimDepart, "");
        if (CollectionUtils.isNotEmpty(array)) {
            JSONObject jsonObject = JSONObject.parseObject(array.get(0).toString());
            //父部门
            Long parentDepartId = jsonObject.getLong("parentDepartId");
            if (null != parentDepartId && parentDepartId != 0) {
                queryDepart(parentDepartId, departList);
            }
        }
        return departList;
    }

}
