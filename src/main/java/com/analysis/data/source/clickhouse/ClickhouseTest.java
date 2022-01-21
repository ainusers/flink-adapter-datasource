package com.analysis.data.source.clickhouse;

import java.util.List;
import java.util.Map;

/**
 * 项目名称: demo
 * 文件名称: com.example.demo.clickhouse
 * 描述: [功能描述]
 * 创建时间: 2021/11/1.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
public class ClickhouseTest {
    public static void main(String[] args) throws Exception {
        ClickhouseSource clickhouseSource = new ClickhouseSource();

        clickhouseSource.open();
        List result = clickhouseSource.send(Map.of(
                "sql", "select * from default.test where id = ? and name = ? and sex = ? and phone = ?",
                "string",Map.of(2,"张三",3,"男"),
                "int",Map.of(1,1,4,132))).run();
        result.forEach(System.out::println);
        clickhouseSource.close();
    }
}
