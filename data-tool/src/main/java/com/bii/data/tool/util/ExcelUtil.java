package com.bii.data.tool.util;

import com.bii.data.tool.consts.ExcelConst;
import java.math.BigDecimal;
import java.util.List;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFCreationHelper;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/15
 */
public class ExcelUtil {

    private static Logger log = LoggerFactory.getLogger(ExcelUtil.class);
    
    private static final String MAIN_SHEET_NAME = "总览";
    
    private static final String FONT_NAME = "微软雅黑";

    
    /**
     * 初始化主sheet页表头
     *
     * @param wb
     * @param mainSheet
     * @param headers
     */
    private static void initMainXHeaders(SXSSFWorkbook wb, Sheet mainSheet, List<String> headers) {
        //表头样式
        CellStyle style = wb.createCellStyle();
        //创建一个居左格式
        style.setAlignment(HorizontalAlignment.LEFT);
        style.setFillBackgroundColor(IndexedColors.BLUE.index);
        //字体样式
        Font fontStyle = wb.createFont();
        fontStyle.setFontName(FONT_NAME);
        fontStyle.setFontHeightInPoints((short) 12);
        fontStyle.setBold(true);
        fontStyle.setColor(IndexedColors.WHITE.index);
        style.setFont(fontStyle);
        //生成sheet1内容
        //第一个sheet的第一行为标题
        Row rowFirst = mainSheet.createRow(0);
        //冻结第一行
        mainSheet.createFreezePane(0, 1, 0, 1);
        //写标题
        for (int i = 0; i < headers.size(); i++) {
            //获取第一行的每个单元格
            Cell cell = rowFirst.createCell(i);
            //设置每列的列宽
            mainSheet.setColumnWidth(i, 4000);
            //加样式
            cell.setCellStyle(style);
            //往单元格里写数据
            cell.setCellValue(headers.get(i));
        }
    }


    /**
     * 创建内容样式
     * @param wb
     * @return
     */
    public static CellStyle createContentCellStyle(SXSSFWorkbook wb) {
        CellStyle cellStyle = wb.createCellStyle();
        // 垂直居中
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        // 水平居中
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        //下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        //左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        //右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        //上边框
        cellStyle.setBorderTop(BorderStyle.THIN);
        // 生成12号字体
        Font font = wb.createFont();
        font.setColor((short)8);
        font.setFontName(FONT_NAME);
        font.setFontHeightInPoints((short) 12);
        cellStyle.setFont(font);
        return cellStyle;
    }

    /**
     * 创建链接样式
     * @param wb
     * @return
     */
    public static CellStyle createMianHeaderStyle(SXSSFWorkbook wb) {

        CellStyle cellStyle = wb.createCellStyle();
        //字体样式
        Font font = wb.createFont();
        font.setFontName(FONT_NAME);
        font.setFontHeightInPoints((short) 12);
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.index);
        //创建一个居左格式
        cellStyle.setAlignment(HorizontalAlignment.LEFT);

        cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        //背景颜色
        cellStyle.setFillForegroundColor(IndexedColors.ROYAL_BLUE.index);
        // 垂直居中
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        // 水平居中
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        //下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        //左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        //右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        //上边框
        cellStyle.setBorderTop(BorderStyle.THIN);
        cellStyle.setFont(font);
        return cellStyle;
    }

    /**
     * 创建链接样式
     * @param wb
     * @return
     */
    public static CellStyle createLinkCellStyle(SXSSFWorkbook wb) {
        CellStyle cellStyle = wb.createCellStyle();
        //创建一个居左格式
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        // 垂直居中
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        // 水平居中
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        //下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        //左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        //右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        //上边框
        cellStyle.setBorderTop(BorderStyle.THIN);
        // 生成12号字体
        Font font = wb.createFont();
        font.setColor((short)8);
        font.setFontHeightInPoints((short) 12);
        font.setUnderline((byte) 1);
        font.setColor(IndexedColors.RED.index);
        cellStyle.setFont(font);
        return cellStyle;
    }

    /**
     * 创建tablesheet标题样式
     * @param wb
     * @return
     */
    public static CellStyle createTableSheetCellStyle(SXSSFWorkbook wb) {
        CellStyle cellStyle = wb.createCellStyle();

        cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        //背景颜色
        cellStyle.setFillForegroundColor(IndexedColors.GREY_40_PERCENT.getIndex());
        //创建一个居左格式
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        // 垂直居中
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        // 水平居中
        cellStyle.setAlignment(HorizontalAlignment.LEFT);
        //下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        //左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        //右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        //上边框
        cellStyle.setBorderTop(BorderStyle.THIN);

        // 生成12号字体
        Font font = wb.createFont();
        font.setColor((short)8);
        font.setFontHeightInPoints((short) 12);
        font.setColor(IndexedColors.WHITE.index);
        cellStyle.setFont(font);
        return cellStyle;
    }


    /**
     *
     * @param row
     * @param column
     * @param val
     * @return
     */
    public static Cell addLinkCell(Row row,
                                   int column,
                                   String sheetName,
                                   String val,
                                   CellStyle cellStyle,
                                   SXSSFCreationHelper createHelper) {
       Cell likeCell = row.createCell(column);
       /* 连接跳转*/
       Hyperlink hyperlink = createHelper.createHyperlink(HyperlinkType.DOCUMENT);
       // "#"表示本文档    "id.val" 表示sheet页名称  "B2"表示第几列第几行
       StringBuilder sb = new StringBuilder();
       //sheetName默认不能超过31
       if (sheetName.length() > 31){
           sheetName = sheetName.substring(0, 31);
       }
       StringBuilder address = sb.append(ExcelConst.HYPERLINK_PREFIX)
                                 .append(sheetName)
                                 .append(ExcelConst.HYPERLINK_SUFFIX);
       hyperlink.setAddress(address.toString());
       likeCell.setHyperlink(hyperlink);
       // 点击进行跳转
       likeCell.setCellValue(val);
       /* 设置为超链接的样式*/
       likeCell.setCellStyle(cellStyle);
       return likeCell;
    }


    /**
     *
     * @param row
     * @param column
     * @param cellStyle
     * @return
     */
    public static Cell addMainSheetLinkCell(Row row,
                                           int column,
                                           CellStyle cellStyle,
                                           SXSSFCreationHelper createHelper) {
        Cell likeCell = row.createCell(column);
        /* 连接跳转*/
        Hyperlink hyperlink = createHelper.createHyperlink(HyperlinkType.DOCUMENT);
        // "#"表示本文档    "val" 表示sheet页名称  "B2"表示第几列第几行
        StringBuilder sb = new StringBuilder();
        StringBuilder address = sb.append(ExcelConst.HYPERLINK_PREFIX)
                .append(ExcelConst.MAIN_SHEET_NAME)
                .append(ExcelConst.HYPERLINK_SUFFIX);
        hyperlink.setAddress(address.toString());
        likeCell.setHyperlink(hyperlink);
        // 点击进行跳转
        likeCell.setCellValue(ExcelConst.RETURN_LINK_VAL);
        /* 设置为超链接的样式*/
        likeCell.setCellStyle(cellStyle);
        return likeCell;
    }


    /**
     * 创建excel单元格
     * @param row 行
     * @param column 列
     * @param val 值
     * @return
     */
    public static Cell addCell(Row row, int column, Object val, CellStyle cellStyle) {
        Cell cell = row.createCell(column);
        try {
            if (val == null) {
                cell.setCellValue("");
            } else if (val instanceof String) {
                cell.setCellValue((String) val);
            } else if (val instanceof Integer) {
                cell.setCellValue((Integer) val);
            } else if (val instanceof Long) {
                cell.setCellValue((Long) val);
            } else if (val instanceof Double) {
                cell.setCellValue((Double) val);
            } else if (val instanceof Float) {
                cell.setCellValue((Float) val);
            } else if (val instanceof BigDecimal) {
                cell.setCellValue(((BigDecimal) val).doubleValue());
            } else if (val instanceof Object) {
                cell.setCellValue(val.toString());
            }
            cell.setCellStyle(cellStyle);
        } catch (Exception e) {
            log.error("Set cell value 【{}, {}】 error: ", row.getRowNum(), column, e);
            cell.setCellValue(val.toString());
        }
        return cell;
    }





}
