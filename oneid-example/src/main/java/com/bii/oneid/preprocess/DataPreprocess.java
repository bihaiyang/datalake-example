package com.bii.oneid.preprocess;

import com.bii.oneid.consts.OneIdConsts;
import com.bii.oneid.consts.OneIdConsts.ErrRelationPairRemove;
import com.bii.oneid.consts.OneIdConsts.IdTypeRelationshipPairConst;
import com.bii.oneid.consts.OneIdConsts.NoiseDataEliminate;
import com.bii.oneid.consts.OneIdConsts.RelationshipWeight;
import com.bii.oneid.entity.IdTypeColumnRelMap;
import com.bii.oneid.entity.OneIdRelationshipPairConfig;
import com.bii.oneid.util.StringUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bihaiyang
 * @desc 数据前置处理
 * @since 2023/09/25
 */
public class DataPreprocess {

    public String execute() {
        StringBuilder sb = new StringBuilder();
        // id 关系对处理
        oneIdRelationshipPairs(sb, null);
        // 关系对权重处理
        oneIdRelationshipWeight(sb);
        //噪音数据消除
        noiseRemoveNode(sb, null, null);
        //异常数据消除
        errRelationshipPairRemove(sb, null, null);
        return sb.toString();
    }

    /**
     * ID 关系对生成
     */
    private String oneIdRelationshipPairs(
            StringBuilder sb,
            List<OneIdRelationshipPairConfig> oneIdRelationshipPairConfigs) {

        sb.append(IdTypeRelationshipPairConst.RELATION_CONFIG_DDL_SCRIPT);
        sb.append(IdTypeRelationshipPairConst.SINGLE_CONFIG_DDL_SCRIPT);

        oneIdRelationshipPairConfigs.forEach(oneIdRelationshipPairConfig -> {

            List<IdTypeColumnRelMap> idTypeAndColRelMaps = oneIdRelationshipPairConfig
                    .getIdTypeAndColRelMaps();

            // single table
            if (idTypeAndColRelMaps.size() < 2) {
                generateSingleWriteScript(oneIdRelationshipPairConfig, sb);
            } else if (OneIdConsts.DWD
                    .equalsIgnoreCase(oneIdRelationshipPairConfig.getTableType())) {
                generateDimWriteScript(sb,
                        IdTypeRelationshipPairConst.DWD_MODEL_RELATION_DEAL_SCRIPT,
                        IdTypeRelationshipPairConst.DWD_MODEL_RELATION_SUB_SCRIPT,
                        oneIdRelationshipPairConfig);
            } else if (OneIdConsts.DIM
                    .equalsIgnoreCase(oneIdRelationshipPairConfig.getTableType())) {
                generateDimWriteScript(sb,
                        IdTypeRelationshipPairConst.DIM_MODEL_RELATION_DEAL_SCRIPT,
                        IdTypeRelationshipPairConst.DIM_MODEL_RELATION_SUB_SCRIPT,
                        oneIdRelationshipPairConfig);
            }
        });
        Map<String, Object> param = new HashMap<>(2);
        param.put("database", "database");
        param.put("biz_date", "biz_date");
        sb.append(StringUtil.replace(IdTypeRelationshipPairConst.ONEID_ALL_KEY_ID, param));
        return sb.toString();
    }

    /**
     * 处理关系权重
     * 
     * @param sb
     */
    private void oneIdRelationshipWeight(StringBuilder sb) {
        StringBuilder str = new StringBuilder();
        str.append(RelationshipWeight.ONEID_RELATION_D_DDL_SCRIPT);
        str.append(RelationshipWeight.ONEID_RELATION_SD_DDL_SCRIPT);
        str.append(RelationshipWeight.ONEID_DWD_RELATION_DEAL_SCRIPT);
        str.append(RelationshipWeight.ONEID_DIM_RELATION_DEAL_SCRIPT);
        Map<String, Object> param = new HashMap<>(2);
        param.put("database", "database");
        param.put("biz_date", "biz_date");
        sb.append(StringUtil.replace(str.toString(), param));
    }

    /**
     * 处理噪音消除规则节点
     *
     * @param ruleValue oneid配置
     */
    private void noiseRemoveNode(StringBuilder sb, String ruleValue, String condition) {
        Map<String, Object> param = new HashMap<>(4);
        param.put("database", "database");
        param.put("bize_date", "biz_date");
        param.put("rule_value", ruleValue);
        param.put("condition", condition);
        sb.append(StringUtil.replace(NoiseDataEliminate.NOISE_DATA_WRITE_TO_GRAPH_INPUT, param));
    }

    private void errRelationshipPairRemove(StringBuilder sb, String condition, String ruleValue){
        String relWeight =
                ErrRelationPairRemove.ONEID_GRAPH_INPUT_ALL +
                ErrRelationPairRemove.ONEID_RELATION_INPUT_SD +
                ErrRelationPairRemove.ERR_REL_DEAL_SCRIPT_STEP1 +
                ErrRelationPairRemove.ERR_REL_DEAL_SUB_SCRIPT_STEP2 +
                ErrRelationPairRemove.ERR_REL_DEAL_SUB_SCRIPT_STEP3 +
                ErrRelationPairRemove.ONEID_OUTPUT_EXTEND_GRAPH +
                ErrRelationPairRemove.ONEID_OUTPUT_EXTEND_SPLIT +
                ErrRelationPairRemove.ONEID_OUTPUT_EXTEND_SCRIPT;
        Map<String, Object> param = new HashMap<>(4);
        param.put("database", "database");
        param.put("condition", condition);
        param.put("rule_value", ruleValue);
        param.put("biz_date", "biz_date");
        sb.append(StringUtil.replace(relWeight, param));
    }
    private void generateSingleWriteScript(
            OneIdRelationshipPairConfig oneIdRelPairConfig, StringBuilder sb) {

        List<IdTypeColumnRelMap> idTypeAndColRelMaps = oneIdRelPairConfig
                .getIdTypeAndColRelMaps();

        idTypeAndColRelMaps.forEach(idTypeColumnRelMap -> {

            Map<String, Object> param = new HashMap<>(6);
            param.put("database", "database");
            param.put("src_table", oneIdRelPairConfig.getTableName());
            param.put("key_id", idTypeColumnRelMap.getCollectRule());
            param.put("key_type", idTypeColumnRelMap.getIdTypeCode());
            param.put("weight", oneIdRelPairConfig.getWeight());
            param.put("biz_date", "date");
            sb.append(StringUtil.replace(IdTypeRelationshipPairConst.SINGLE_RELATION_WRITE_SCRIPT, param));
        });
    }

    /**
     * 生成处理维度表\事实表脚本 ;
     *
     * @param sb
     * @param subSql
     * @param insertSql
     * @param oneIdRelPairConfig
     */
    private void generateDimWriteScript(
            StringBuilder sb,
            String subSql,
            String insertSql,
            OneIdRelationshipPairConfig oneIdRelPairConfig) {
        List<IdTypeColumnRelMap> idTypeAndColRelMaps = oneIdRelPairConfig.getIdTypeAndColRelMaps();
        List<String> subSqls = new ArrayList();

        for (int i = 0; i < idTypeAndColRelMaps.size(); i++) {
            IdTypeColumnRelMap leftRelMap = idTypeAndColRelMaps.get(i);
            for (int j = i + 1; j < idTypeAndColRelMaps.size(); j++) {
                IdTypeColumnRelMap rightRelMaq = idTypeAndColRelMaps.get(j);
                Map<String, Object> param = new HashMap<>(7);
                param.put("left_key_type", leftRelMap.getIdTypeCode());
                param.put("left_key_id", leftRelMap.getIdTypeAttribute());
                param.put("right_key_type", rightRelMaq.getIdTypeCode());
                param.put("right_key_id", rightRelMaq.getCollectRule());
                param.put("weight", oneIdRelPairConfig.getWeight());
                param.put("database", "database");
                param.put("src_table", oneIdRelPairConfig.getTableName());
                subSqls.add(StringUtil.replace(subSql, param));
            }
        }
        String sql = String.format(
                insertSql, String.join(OneIdConsts.UNION_ALL, subSqls));
        Map<String, Object> param = new HashMap<>(3);
        param.put("database", "database");
        param.put("src_table", oneIdRelPairConfig.getTableName());
        param.put("biz_date", "biz_date");
        sb.append(StringUtil.replace(sql, param));
    }

}
