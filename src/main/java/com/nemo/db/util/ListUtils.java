/* 
 * All rights Reserved, Designed By 微迈科技
 * 2018/1/10 16:16
 */
package com.nemo.db.util;

import java.util.Collection;

/**
 * Created by Nemo on 2018/1/10.
 */
public class ListUtils {

    public static boolean isEmpty(Collection collection){
        if(collection == null || collection.isEmpty()){
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(Collection collection){
        return !isEmpty(collection);
    }

}
