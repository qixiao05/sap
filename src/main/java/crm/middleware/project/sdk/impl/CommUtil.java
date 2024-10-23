package crm.middleware.project.sdk.impl;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Component
public class CommUtil {

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
