package com.bii.example.hudi.flink.service.sql;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

/**
 * @fileName: SQLOperator.java
 * @description: sql操作符
 * @author: huangshimin
 * @date: 2021/11/18 5:14 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SQLOperator {
    private List<String> ddlSQLList;
    private List<String> querySQLList;
    private List<String> insertSQLList;

    public void checkParams() {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(ddlSQLList), "ddlSqlList不能为空");
    }
}

