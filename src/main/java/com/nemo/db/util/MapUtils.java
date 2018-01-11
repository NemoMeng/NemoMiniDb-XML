/* 
 * All rights Reserved, Designed By 微迈科技
 * 2018/1/10 16:24
 */
package com.nemo.db.util;

import java.util.Map;

/**
 * Created by Nemo on 2018/1/10.
 */
public class MapUtils {

    public static boolean isEmpty(Map map){
        if(map == null || map.isEmpty()){
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(Map map){
        return !isEmpty(map);
    }

}
