package com.analysis.data.source.clickhouse;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 项目名称: sirius-sdk
 * 文件名称: com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch
 * 描述: [离线分析source统一接口]
 * 创建时间: 2021/10/27.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
public interface SourceFunction<T> extends Serializable {

    /**
      * @author: tianyong
      * @time: 2021/10/26 18:06
      * @description: 创建client链接,赋值相关属性
      * @Version: v1.0
      * @company: xxx Group.Situation xxx事业部
      */
    void open() throws Exception;

    /**
      * @author: tianyong
      * @time: 2021/10/26 18:06
      * @description: 运行查询服务
      * @Version: v1.0
      * @company: xxx Group.Situation xxx事业部
      */
    List<String> run() throws Exception;

    /**
     * @author: tianyong
     * @time: 2021/10/26 18:07
     * @description: 发送参数信息
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    T send(Map<String,Object> params) throws Exception;

    /**
      * @author: tianyong
      * @time: 2021/10/26 18:07
      * @description: 关闭client链接
      * @Version: v1.0
      * @company: xxx Group.Situation xxx事业部
      */
    void close() throws Exception;

}
