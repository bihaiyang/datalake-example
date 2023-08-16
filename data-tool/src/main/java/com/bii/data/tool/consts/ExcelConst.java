package com.bii.data.tool.consts;

/**
 * @author bihaiyang
 */
public class ExcelConst {
    /**
     * table sheet header size
     */
    public static final int TABLE_SHEET_COLUMN_SIZE = 15;
    /**
     * 主sheet页表头信息
     */
    public static final String[] MAIN_SHEET_HEADER = {"数据模型名称", "数据模型描述"};
    /**
     * 表meta信息sheet页第一部分表头信息
     */
    public static final String[] TABLE_SHEET_ONE_PART = {"数据模型名", "数据模型描述"};
    /**
     * 表meta信息sheet页第二部分表头信息
     */
    public static final String[] TABLE_SHEET_TWO_PART = {"字段序号", "字段", "字段说明", "字段类型", "是否主键","允许为空", "是否唯一"};
    /**
     * 表meta信息sheet页第三部分表头信息
     */
    public static final String TABLE_SHEET_THREE_PART = "DDL";
    /**
     * 主sheet列宽
     */
    public static final int[] MAIN_SHEET_WIDTH = {33*256, 25*256};
    /**
     * table sheet列宽
     */
    public static final int[] TABLE_SHEET_WIDTH = {10*256, 10*256, 19*256, 10*256, 10*256, 10*256,  10*256};
    /**
     * 主sheet页名称
     */
    public static final String MAIN_SHEET_NAME = "总览";
    /**
     * 返回
     */
    public static final String RETURN_LINK_VAL = "返回";
    /**
     * 微软雅黑
     */
    public static final String FONT_NAME_WRYH = "微软雅黑";
    /**
     * 等线
     */
    public static final String FONT_NAME_DX = "等线";
    /**
     * 超链接 本文档
     */
    public static final String HYPERLINK_PREFIX = "#";
    /**
     * 链接至B2单元格
     */
    public static final String HYPERLINK_SUFFIX = "!B2";

    /**
     * 失效时间7天
     */
    public static final long EXPIRATION_TIME = 7 * 24 * 3600 * 1000;
    /**
     * 文件后缀
     */
    public static final String XLSX_SUFFIX = ".xlsx";
}
