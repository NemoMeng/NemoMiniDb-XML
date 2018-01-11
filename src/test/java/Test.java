/* 
 * All rights Reserved, Designed By 微迈科技
 * 2018/1/11 10:16
 */

import com.nemo.db.DbCore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Nemo on 2018/1/11.
 */
public class Test {

    //测试表名
    private String tableName = "t_user";
    //数据库文件存放位置
    private String path = "f:/database";
    //数据库名称
    private String dbName = "test";

    private DbCore core = new DbCore(path,dbName);

    @org.junit.Test
    public void createTable(){
        Map<String,Class> struct = new HashMap<>();
        struct.put("id",Integer.class);
        struct.put("name",String.class);
        struct.put("age",Integer.class);
        struct.put("sex",String.class);
        core.createTable(tableName,struct);

        core.dropTable(tableName);
    }

    @org.junit.Test
    public void dropTable(){
        core.dropTable(tableName);
    }

    @org.junit.Test
    public void insert(){
        for(int i=0;i<500;i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", i);
            data.put("name", "NemoMeng"+i);
            data.put("age", 18);
            data.put("sex", "男");
            core.insert(data, tableName);
        }
    }

    @org.junit.Test
    public void delete(){
        Map<String, Object> data = new HashMap<>();
        data.put("name", "NemoMeng1");
        core.delete(data,tableName);
    }

    @org.junit.Test
    public void update(){
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("name", "NemoMeng1");

        Map<String, Object> newData = new HashMap<>();
        newData.put("id", 6666);
        newData.put("name", "帅哥Nemo");
        newData.put("age", 18);
        newData.put("sex", "男");

        core.update(queryMap,newData,tableName);
    }

    @org.junit.Test
    public void find(){
        //查询条件
        Map<String,Object> queryMap = new HashMap<>();
        queryMap.put("age","18");

        //开始查询
        List<Map<String, Object>> maps = core.find(queryMap,tableName);
        int i=1;
        for(Map<String,Object> map : maps){
            System.out.print(i+"        ");
            for(String key : map.keySet()){
                System.out.print(key + ":"+map.get(key) +"        ");
            }
            System.out.println();
            i++;
        }
    }
}
