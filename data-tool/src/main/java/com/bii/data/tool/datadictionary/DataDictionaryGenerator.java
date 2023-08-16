package com.bii.data.tool.datadictionary;

import com.bii.data.tool.consts.ExcelConst;
import com.bii.data.tool.datadictionary.data.AbstractDataCollector;
import com.bii.data.tool.datadictionary.data.AbstractDataSourceParam;
import com.bii.data.tool.datadictionary.data.Column;
import com.bii.data.tool.datadictionary.data.DataCollectorFactory;
import com.bii.data.tool.datadictionary.data.MysqlParam;
import com.bii.data.tool.datadictionary.data.Table;
import com.bii.data.tool.util.ExcelUtil;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFCreationHelper;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

/**
 * @author bihaiyang
 * @desc 数据字典生成器
 * @since 2023/08/15
 */
@Slf4j
public class DataDictionaryGenerator {
    
    private SXSSFWorkbook workbook;
    
    private SXSSFCreationHelper createHelper;
    
    private AbstractDataCollector dataCollector;
    
    private List<Table> tables;
    
    private Map<String, List<Column>> columnsMap;
    
    
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        
        DataDictionaryGenerator dataDictionaryGenerator = new DataDictionaryGenerator();
        MysqlParam mysqlParam = new MysqlParam();
        mysqlParam.setUrl("jdbc:mysql://**:3306/terp_staging");
        mysqlParam.setSchema("**");
        mysqlParam.setPassword("**");
        mysqlParam.setUser("**");
        mysqlParam.setDriverClassName("com.mysql.cj.jdbc.Driver");
        mysqlParam.setFilePath("terp_staging_20230816_dic.xlxs");
        mysqlParam.setType("mysql");
        SXSSFWorkbook workbook = dataDictionaryGenerator.createDataDicXlsx(mysqlParam);
        try (FileOutputStream fileOutputStream = new FileOutputStream(mysqlParam.getFilePath())) {
            workbook.write(fileOutputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public SXSSFWorkbook createDataDicXlsx(AbstractDataSourceParam param)
            throws SQLException, ClassNotFoundException {
        workbook = new SXSSFWorkbook();
        createHelper = new SXSSFCreationHelper(workbook);
        dataCollector = DataCollectorFactory.newInstance(param);
        tables = dataCollector.listTables();
        columnsMap = dataCollector
                .listTableColumns(tables.stream().map(Table::getName).collect(Collectors.toList()));
        //创建主sheet页
        createMainSheet();
        //创建tableInfo sheet页
        createTableSheet();
        return workbook;
    }

    
    /**
     * 创建主sheet页
     */
    private void createMainSheet(){
        //获取样式
        CellStyle mainHeaderStyle = ExcelUtil.createMianHeaderStyle(workbook);
        CellStyle linkCellStyle = ExcelUtil.createLinkCellStyle(workbook);
        CellStyle contentCellStyle = ExcelUtil.createContentCellStyle(workbook);
        //创建总览sheet页
        SXSSFSheet mainSheet = workbook.createSheet(ExcelConst.MAIN_SHEET_NAME);
        //设置无边框
        mainSheet.setDisplayGridlines(false);
        createMainSheetHeader(mainSheet, mainHeaderStyle);
        createMainSheetValue(mainSheet, linkCellStyle, contentCellStyle);
    }
    
    /**
     * 创建主sheet页标题行
     * @param mainSheet
     * @param mainHeaderStyle
     */
    private void createMainSheetHeader(SXSSFSheet mainSheet, CellStyle mainHeaderStyle){
        String[] mainSheetHeader = ExcelConst.MAIN_SHEET_HEADER;
        int[] mainSheetWidth = ExcelConst.MAIN_SHEET_WIDTH;
        //创建标题行
        Row rowFirst = mainSheet.createRow(1);
        //写标题
        for (int i = 1; i <= mainSheetHeader.length; i++) {
            //获取第一行的每个单元格
            Cell cell = rowFirst.createCell(i);
            //设置每列的列宽
            mainSheet.setColumnWidth(i, mainSheetWidth[i - 1]);
            //加样式
            cell.setCellStyle(mainHeaderStyle);
            //往单元格里写数据
            cell.setCellValue(mainSheetHeader[i - 1]);
        }
    }
    
    
    /**
     * 创建主sheet页值行
     * @param mainSheet
     * @param
     */
    private void createMainSheetValue(SXSSFSheet mainSheet,
            CellStyle linkCellStyle,
            CellStyle contentCellStyle){
        //写标题
        for (int i = 2; i < tables.size() + 2; i++) {
            //创建行
            Table table = tables.get(i - 2);
            Row row = mainSheet.createRow(i);
            ExcelUtil.addLinkCell(row, 1, table.getName(),
                    table.getName(), linkCellStyle, createHelper);
            ExcelUtil.addCell(row, 2, table.getComment(), contentCellStyle);
        }
    }
    
    public void createTableSheet(){
        for (int i = 0; i < tables.size(); i++ ){
            Table table = tables.get(i);
            SXSSFSheet sheet = workbook.createSheet(table.getName());
            sheet.setDisplayGridlines(false);
            //创建第一部分
            createTableSheetOnePart(sheet, table);
            createTableSheetTwoPart(sheet, table);
        }
        
    }
    
    /**
     * 创建第一部分单元格
     * @param table
     */
    public void createTableSheetOnePart(SXSSFSheet sheet, Table table){
        CellStyle tableSheetCellStyle = ExcelUtil.createTableSheetCellStyle(workbook);
        CellStyle contentCellStyle = ExcelUtil.createContentCellStyle(workbook);
        CellStyle linkCellStyle = ExcelUtil.createLinkCellStyle(workbook);
        String[] tableSheetOnePart = ExcelConst.TABLE_SHEET_ONE_PART;
        
        for (int i = 1; i <= tableSheetOnePart.length; i++){
            Row row = sheet.createRow(i);
            for (int j = 1; j <= ExcelConst.TABLE_SHEET_COLUMN_SIZE; j++){
                if (j == 1){
                    ExcelUtil.addCell(row, j, tableSheetOnePart[i - 1], tableSheetCellStyle);
                } else if(j == 3 && i == 1){
                    ExcelUtil.addCell(row, j, table.getName(), contentCellStyle);
                }else if(j == 3 && i == 2){
                    ExcelUtil.addCell(row, j, table.getComment(), contentCellStyle);
                }else if(j == 15 && i == 1){
                    ExcelUtil.addMainSheetLinkCell(row, j, linkCellStyle, createHelper);
                }else{
                    ExcelUtil.addCell(row, j, "", contentCellStyle);
                }
            }
            //合并单元格
            sheet.addMergedRegion(new CellRangeAddress(i, i, 1, 2));
            sheet.addMergedRegion(new CellRangeAddress(i, i, 3, 14));
        }
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 15, 15));
    }
    
    /**
     * 创建第二部分cell
     * @param table
     */
    public void createTableSheetTwoPart(SXSSFSheet sheet, Table table){
        CellStyle tableSheetCellStyle = ExcelUtil.createTableSheetCellStyle(workbook);
        CellStyle contentCellStyle = ExcelUtil.createContentCellStyle(workbook);
        CellStyle mainHeaderStyle = ExcelUtil.createMianHeaderStyle(workbook);
    
        List<Column> columns = columnsMap.get(table.getName());
        createTableSheetHeader(sheet, tableSheetCellStyle, mainHeaderStyle);
        if (!CollectionUtils.isEmpty(columns)){
            createTableSheetValue(sheet, contentCellStyle, columns);
        }
    }
    
    /**
     * 创建主sheet页标题行
     * @param Sheet
     * @param tableSheetCellStyle
     */
    private void createTableSheetHeader(SXSSFSheet Sheet, CellStyle tableSheetCellStyle, CellStyle mainHeaderStyle){
        String[] tableSheetTwoPart = ExcelConst.TABLE_SHEET_TWO_PART;
        int[] tableSheetWidth = ExcelConst.TABLE_SHEET_WIDTH;
        //创建标题行
        Row rowFirst = Sheet.createRow(4);
        //写标题
        for (int i = 1; i <= tableSheetTwoPart.length; i++) {
            //获取第一行的每个单元格
            Cell cell = rowFirst.createCell(i);
            //设置每列的列宽
            Sheet.setColumnWidth(i, tableSheetWidth[i - 1]);
            //加样式
            if (i == 15 || i == 14 ){
                cell.setCellStyle(mainHeaderStyle);
            }else {
                cell.setCellStyle(tableSheetCellStyle);
            }
            //往单元格里写数据
            cell.setCellValue(tableSheetTwoPart[i - 1]);
        }
    }
    
    /**
     * 创建table sheet页meta 部分
     * @param Sheet
     * @param contentCellStyle
     */
    private void createTableSheetValue(SXSSFSheet Sheet,
            CellStyle contentCellStyle,
            List<Column> columns){
        
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Row row = Sheet.createRow(i + 5);
            ExcelUtil.addCell(row, 1, i, contentCellStyle);
            ExcelUtil.addCell(row, 2, column.getColumnName(), contentCellStyle);
            ExcelUtil.addCell(row, 3, column.getColumnComment(), contentCellStyle);
            ExcelUtil.addCell(row, 4, column.getColumnType(), contentCellStyle);
            ExcelUtil.addCell(row, 5, column.getColumnKey(), contentCellStyle);
            ExcelUtil.addCell(row, 6, column.getIsNullable(), contentCellStyle);
            ExcelUtil.addCell(row, 7, column.getColumnUnique(), contentCellStyle);
          
        }
    }
    
    /**
     * 创建第二部分cell
     * @param sheet
     * @param table
     */
    public void createTableSheetThreePart(SXSSFSheet sheet, Table table){
        CellStyle tableSheetCellStyle = ExcelUtil.createTableSheetCellStyle(workbook);
        CellStyle contentCellStyle = ExcelUtil.createContentCellStyle(workbook);
        String DDL = ExcelConst.TABLE_SHEET_THREE_PART;
        int rowIndex = 5;
      
        for (int i = 1; i <= ExcelConst.TABLE_SHEET_COLUMN_SIZE; i++){
            Row row = sheet.createRow(i + rowIndex);
            for (int j = 1; j <= ExcelConst.TABLE_SHEET_COLUMN_SIZE; j++){
                if (j == 1 && i == 1){
                    //第一行 第2列
                    ExcelUtil.addCell(row, j, DDL, tableSheetCellStyle);
                }else if (j == 3 && i == 1){
                    //第一行 第4列
                    ExcelUtil.addCell(row, j, table.getDdl(), contentCellStyle);
                }else {
                    ExcelUtil.addCell(row, j, "", contentCellStyle);
                }
            }
        }
        //合并单元格
        sheet.addMergedRegion(new CellRangeAddress(rowIndex + 1, rowIndex + 15, 1, 2));
        sheet.addMergedRegion(new CellRangeAddress(rowIndex + 1, rowIndex + 15, 3, 15));
        
    }
    
    
}
