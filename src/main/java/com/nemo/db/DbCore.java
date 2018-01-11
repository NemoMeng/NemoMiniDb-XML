/* 
 * All rights Reserved, Designed By 微迈科技
 * 2018/1/11 9:55
 */
package com.nemo.db;

import com.nemo.db.util.ListUtils;
import com.nemo.db.util.MapUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库核心
 * Created by Nemo on 2018/1/11.
 */
public class DbCore {

    //数据库路径
    private String path;
    //数据库名称
    private String dbName;
    //数据库表列表
    private Map<String,File> tableMap = new HashMap<>();
    //数据库表结构集合
    private Map<String,Map<String,String>> structMap = new HashMap<>();

    /**
     * 初始化数据库
     * @param path
     * @param dbName
     */
    public DbCore(String path,String dbName){
        this.path = path;
        this.dbName = dbName;

        File dbDir = new File(path+"/"+dbName);
        if(!dbDir.exists()){
            //不存在则创建
            dbDir.mkdirs();
        }

        //加载所有表
        loadTables(dbDir);
    }

    /**
     * 创建表
     * @param tableName 表名称
     * @param columns   表列集合：名称-->类型
     */
    public void createTable(String tableName,Map<String,Class> columns){
        File tableFile = getTable(tableName);
        if(tableFile!=null){
            throw new RuntimeException("抱歉，该表名称已被使用");
        }

        if(MapUtils.isEmpty(columns)){
            throw new RuntimeException("不能创建空表");
        }

        File file = new File(path+"/"+dbName+"/"+tableName+".xml");
        try {
            //创建表格文件
            file.createNewFile();

            //增加根节点
            Document document = new Document();
            Element rootElement = new Element("table");
            document.setRootElement(rootElement);

            //基本结构存储
            Element structElement = new Element("struct");
            Map<String,String> map = new HashMap<>();
            for(String key : columns.keySet()){
                Element element = new Element("column");
                element.setAttribute("name",key);
                element.setAttribute("type",columns.get(key).getName());
                structElement.addContent(element);
                map.put(key,columns.get(key).getName());
            }
            rootElement.addContent(structElement);

            //基本数据根结点
            Element dataElement = new Element("data");
            rootElement.addContent(dataElement);

            XMLOutputter out = new XMLOutputter();
            FileOutputStream outputStream = new FileOutputStream(file);
            out.output(document,outputStream);

            //存放到内存中
            tableMap.put(tableName,file);
            structMap.put(tableName,map);
        } catch (IOException e) {
           throw new RuntimeException("抱歉，创建表失败，请重试");
        }
    }

    /**
     * 删除表
     * @param tableName
     */
    public void dropTable(String tableName){
        File tableFile = getTable(tableName);
        if(tableFile==null){
            throw new RuntimeException("抱歉，找不到需要删除的表");
        }
        tableFile.delete();
        tableMap.remove(tableName);
        structMap.remove(tableName);
    }

    /**
     * 插入数据
     * @param dataMap
     * @param tableName
     */
    public void insert(Map<String,Object> dataMap,String tableName){
        File table = getTable(tableName);
        if(table == null){
            throw new RuntimeException("抱歉，找不到指定的表");
        }

        //TODO 验证数据结构

        //构建数据
        try {
            SAXBuilder builder = new SAXBuilder();
            Document document = builder.build(table);
            Element rootElement = document.getRootElement();
            Element dataElement = rootElement.getChild("data");

            Map<String,String> struct = structMap.get(tableName);
            Element singleElement = new Element("singleData");
            for(String key:struct.keySet()){
                Object value = dataMap.get(key);
                Element element = new Element(key);
                element.setText(value.toString());
                singleElement.addContent(element);
            }
            dataElement.addContent(singleElement);

            //保存到文件
            XMLOutputter out = new XMLOutputter();
            FileOutputStream outputStream = null;
            outputStream = new FileOutputStream(table);
            out.output(document,outputStream);
        } catch (Exception e) {
            throw new RuntimeException("抱歉，加载表失败");
        }
    }

    /**
     * 数据删除
     * @param dataMap
     * @param tableName
     */
    public int delete(Map<String,Object> dataMap, String tableName){
        File table = getTable(tableName);
        if(table == null){
            throw new RuntimeException("抱歉，找不到指定的表");
        }

        int deleteCount = 0;
        try {
            SAXBuilder builder = new SAXBuilder();
            Document document = builder.build(table);
            Element rootElement = document.getRootElement();
            Element dataElement = rootElement.getChild("data");

            List<Element> needsDeleteElements = new ArrayList<>();
            for(String key : dataMap.keySet()){
                List<Element> children = dataElement.getChildren("singleData");
               if(ListUtils.isNotEmpty(children)){
                   for(Element element : children){
                       Element kid = element.getChild(key);
                       if(kid.getText().equals(dataMap.get(key).toString())){
                           needsDeleteElements.add(element);
                       }
                   }
               }
            }


            for(Element element : needsDeleteElements){
                dataElement.removeContent(element);
                deleteCount ++;
            }

            //保存到文件
            XMLOutputter out = new XMLOutputter();
            FileOutputStream outputStream = null;
            outputStream = new FileOutputStream(table);
            out.output(document,outputStream);
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("抱歉，加载表失败");
        }
        return deleteCount;
    }

    /**
     * 数据更新
     * @param queryMap
     * @param newData
     * @param tableName
     */
    public int update(Map<String,Object> queryMap, Map<String,Object> newData, String tableName){
        //先删除，记录数量
        int count = delete(queryMap,tableName);
        //再新增
        for(int i=0;i<count;i++){
            insert(newData,tableName);
        }

        return count;
    }

    /**
     * 数据查询
     * @param queryMap
     * @param tableName
     * @return
     */
    public List<Map<String, Object>> find(Map<String,Object> queryMap, String tableName){

        File table = getTable(tableName);
        if(table == null){
            throw new RuntimeException("抱歉，找不到指定的表");
        }

        List<Map<String,Object>> result = new ArrayList<>();
        try {
            SAXBuilder builder = new SAXBuilder();
            Document document = builder.build(table);
            Element rootElement = document.getRootElement();
            Element dataElement = rootElement.getChild("data");
            List<Element> children = dataElement.getChildren("singleData");
            for(Element element : children){
                List<Element> kids = element.getChildren();
                Map<String,Object> map = new HashMap<>();
                for(Element kid : kids){
                    map.put(kid.getName(),kid.getText());
                }
                if(MapUtils.isEmpty(queryMap) || match(map,queryMap)){
                    result.add(map);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("抱歉，加载表失败");
        }

        return result;
    }

    /**
     * 加载已有表
     * @param dbDir
     */
    private void loadTables(File dbDir){
        File[] tableFiles = dbDir.listFiles();
        tableMap = new HashMap<>();
        if(tableFiles!=null){
            for(File file : tableFiles){
                String fileName = file.getName().toLowerCase();
                if(fileName.endsWith(".xml")) {
                    String tableName = fileName.replace(".xml","");
                    SAXBuilder saxBuilder = new SAXBuilder();
                    try {
                        Document document = saxBuilder.build(file);
                        Element rootElement = document.getRootElement();
                        Element structElement = rootElement.getChild("struct");
                        List<Element> columns = structElement.getChildren("column");
                        if(ListUtils.isNotEmpty(columns)){
                            Map<String,String> map = new HashMap<>();
                            for(Element column : columns){
                                String name = column.getAttributeValue("name");
                                String type = column.getAttributeValue("type");
                                map.put(name,type);
                            }
                            structMap.put(tableName,map);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("抱歉，加载表失败");
                    }
                    tableMap.put(tableName, file);
                }
            }
        }
    }

    /**
     * 判断第一个参数中的属性是否符合第二个参数的条件。
     * @param data 被判断数据对象
     * @param queryMap 条件集合对象
     * @return 当第二个参数中所有属性都可以在第一个参数中找到，并且属性值与之相等时，返回true，否则false。
     */
    private boolean match(Map<String,Object> data, Map<String,Object> queryMap){
        boolean result = true;
        for(Object key: queryMap.keySet()){
            if(data.containsKey(key) && data.get(key).equals(queryMap.get(key))){
                continue;
            }else{
                result = false;
                break;
            }
        }
        return result;
    }

    /**
     * 得到一张表的文件
     * @param tableName
     * @return
     */
    private File getTable(String tableName){
        if(tableName == null){
            throw new RuntimeException("请输入表名");
        }
        return tableMap.get(tableName);
    }
}
