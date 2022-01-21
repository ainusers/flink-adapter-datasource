package com.analysis.data.source.clickhouse;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 项目名称: demo
 * 文件名称: com.example.demo.clickhouse
 * 描述: [功能描述]
 * 创建时间: 2021/10/29.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
public class ClickhouseService implements Serializable {

    /**
      * @time: 2021/11/1 17:43
      * @Param: statement对象、params变量参数
      * @return: clickhouse查询结果值
      * @Description: 运行clickhouse查询事件
      * @Description: 注：目前暂时支持string、int
      */
    public static List<String> searchData(ClickHousePreparedStatement statement,Map<String,Object> params) throws Exception{
        // 赋值变量参数
        statement = voluation(statement, params);
        // 执行查询语句
        ResultSet rs = statement.executeQuery();
        // 从结果集中提取数据
        List<String> list = new ArrayList();
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            Map map = new HashMap();
            for (int i = 1,length=rsmd.getColumnCount(); i <= length; i++) {
                String columnTypeName = rsmd.getColumnTypeName(i).toLowerCase();
                if(StringUtils.equals("string",columnTypeName)){
                    map.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
                }else if(columnTypeName.contains("int")){
                    map.put(rsmd.getColumnName(i), rs.getInt(rsmd.getColumnName(i)));
                }
            }
            list.add(JSON.toJSONString(map));
        }
        rs.close();
        return list;
    }
    
    /**
      * @time: 2021/11/1 15:12
      * @Param: statement对象、变量参数
      * @return: statement对象
      * @Description: 赋值sql语句变量参数
      * @Description: 注：目前暂时支持string、int
      */
    private static ClickHousePreparedStatement voluation(ClickHousePreparedStatement statement,Map<String,Object> params) throws Exception{
        // 赋值变量属性
        params.forEach((key,value)->{
            key = key.toLowerCase();
            if(StringUtils.equals("string",key)){
                ((Map) value).forEach((index,param)->{
                    try {
                        statement.setString((int)index,String.valueOf(param));
                    } catch (SQLException throwables) {
                        System.out.println("赋值sql语句string类型变量参数 失败!");
                        throwables.printStackTrace();
                    }
                });
            }else if(key.contains("int")){
                ((Map) value).forEach((index,param)->{
                    try {
                        statement.setInt((int)index,(int)param);
                    } catch (SQLException throwables) {
                        System.out.println("赋值sql语句int类型变量参数 失败!");
                        throwables.printStackTrace();
                    }
                });
            }
        });
        // 打印填充后的SQL语句
        System.out.println("execute sql: " + statement.asSql());
        return statement;
    }

}
