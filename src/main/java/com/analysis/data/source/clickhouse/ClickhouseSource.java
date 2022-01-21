package com.analysis.data.source.clickhouse;

import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
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
public class ClickhouseSource<T> implements SourceFunction<ClickhouseSource<T>>, Serializable {

    private Connection connect;
    private Map<String,Object> params;
    private ClickHousePreparedStatement statement;

    /**
      * @time: 2021/10/29 16:54
      * @Param: 
      * @return: 
      * @Description: 初始化clickhouse-connect连接，赋值相关属性
      */
    @Override
    public void open() throws Exception {
        // 注册clickhouse-JDBC驱动
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        // 初始化链接
        connect = DriverManager.getConnection("jdbc:clickhouse://192.168.43.43:8123/default", "default", "root1234");
    }

    /**
      * @time: 2021/10/29 16:54
      * @Param: 
      * @return: 查询结果集
      * @Description: 运行clickhouse查询事件
      */
    @Override
    public List<String> run() throws Exception {
        return com.example.demo.clickhouse.ClickhouseService.searchData(statement,params);
    }

    /**
     * @time: 2021/10/27 14:48
     * @Param: params变量参数
     * @return: 当前类对象
     * @Description: 发送参数信息
     */
    @Override
    public com.example.demo.clickhouse.ClickhouseSource<T> send (Map<String,Object> param) throws Exception {
        params = (Map<String,Object>)param;
        statement = (ClickHousePreparedStatement) connect.prepareStatement((String) param.get("sql"));
        return this;
    }

    /**
      * @time: 2021/10/29 16:54
      * @Param:
      * @return: 
      * @Description: 关闭connect链接
      */
    @Override
    public void close() throws Exception {
        statement.close();
        connect.close();
    }
}
