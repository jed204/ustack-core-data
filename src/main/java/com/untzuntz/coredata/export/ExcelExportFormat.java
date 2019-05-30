package com.untzuntz.coredata.export;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.untzuntz.coredata.ExportFormat;
import com.untzuntz.coredata.exceptions.FailedRequestException;
import com.untzuntz.ustack.main.UFile;

public class ExcelExportFormat implements ExportFormat,OutputCallback {

	private UFile outputFile;
	private Workbook wb;
	private Sheet sheet;
	private Row row;
	private CreationHelper createHelper;
	private OutputStream out;
	private List<ExportFieldFormat> fields;
	private Map<String,CellStyle> formats;
	private int rowNumber;
	private int columnNumber;
	
	public int getRowNumber() {
		return rowNumber;
	}
	
	public int getColumnNumber() {
		return columnNumber;
	}
	
	/**
	 * Exports the data to a CSV File
	 * 
	 */
	public ExcelExportFormat(UFile outputFile)
	{
		this.outputFile = outputFile;
		this.fields = new ArrayList<ExportFieldFormat>();
		this.formats = new HashMap<String,CellStyle>();
	}
	
	public void addFields(List<ExportFieldFormat> newFields) {
		if (newFields == null)
			return;
		this.fields.addAll(newFields);
	}
	
	public boolean hasFieldName(String name)
	{
		for (ExportFieldFormat field : fields)
		{
			if (field.getFieldName().equals(name))
				return true;
		}
		return false;
	}

	public void addField(ExportFieldFormat field) {
		if (field == null)
			return;
		fields.add(field);
	}

	@Override
	public void start() throws FailedRequestException {
		
		wb = new SXSSFWorkbook();
		sheet = wb.createSheet("Report");
		createHelper = wb.getCreationHelper();
	    
		try {
			// open the output file
			out = outputFile.getOutputStream();
			startLine();
		
			CellStyle headerStyle = getHeaderStyle();
			
			// write the header
			for (int fieldIdx = 0; fieldIdx < fields.size(); fieldIdx++)
			{
				ExportFieldFormat eff = fields.get(fieldIdx);
				writeField(eff.getHeaderName(), eff, headerStyle);
				nextField();
			}
			
			nextLine();
			
		} catch (Exception e) {
			throw new FailedRequestException(e);
		}
	}

	/**
	 * Defines the default header style
	 * 
	 * @return
	 */
	private CellStyle getHeaderStyle() {
		
		CellStyle headerStyle = wb.createCellStyle();
		
		// AQUA background
		headerStyle.setFillForegroundColor(IndexedColors.AQUA.getIndex());
		headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

		// Bold text
		Font font = wb.createFont();
		font.setBold(true);
	    headerStyle.setFont(font);
	    
	    return headerStyle;
	    
	}
	
	@Override
	public void output(DBCursor cur) throws FailedRequestException {
		
		if (cur == null)
			return;
		
		try {
			// grab each object from the DBCursor
			while (cur.hasNext())
			{
				DBObject dbObject = cur.next();
				output(dbObject);
			}
		} catch (Exception e) {
			ExportUtil.close(out);
		}
	}
	
	@Override
	public void output(DBObject dbObject) throws Exception {
		// send it to the multiline writer - we get callback to do the actual writing (see nextField, writeField, and nextLine)
		ExportUtil.writeMultiLine(this, fields, dbObject);
	}

	@Override
	public void finished() throws FailedRequestException {
	    try {
			wb.write(out);
		} catch (IOException e) {
			throw new FailedRequestException(e);
		}
		// close the output file
	    ExportUtil.close(out);
	}

	@Override
	public void nextField() throws Exception {
		columnNumber++;
	}

	@Override
	public void writeField(Object fieldValue, ExportFieldFormat format) throws Exception {
		writeField(fieldValue, format, null);
	}
	
	/**
	 * Writes the data to the row
	 * 
	 */
	private void writeField(Object fieldValue, ExportFieldFormat format, CellStyle overrideStyle)
	{
		Cell cell = row.createCell(columnNumber);
		if (fieldValue instanceof String)
		{
			cell.setCellValue((String)fieldValue);
			if (format != null)
				cell.setCellStyle(getStyleByColor(format.getForegroundColor(), format.getBackgroundColor()));
		}
		else if (fieldValue instanceof Date)
		{
			cell.setCellValue((Date)fieldValue);
			cell.setCellStyle(getStyleByFormat(format.getFieldFormat()));
		}
		else if (fieldValue instanceof Number)
		{
			cell.setCellValue(((Number)fieldValue).doubleValue());
			cell.setCellStyle(getStyleByFormat(format.getFieldFormat()));
		}
		if (overrideStyle != null)
			cell.setCellStyle(overrideStyle);
	}
	
	@Override
	public void startLine() throws Exception {
		row = sheet.createRow(rowNumber);
		columnNumber = 0;
	}

	@Override
	public void nextLine() throws Exception {
		rowNumber++;
	}
	
	/**
	 * Cache the data formats
	 * 
	 */
	private CellStyle getStyleByFormat(String format)
	{
		CellStyle style = formats.get(format);
		if (style == null)
		{
			style = wb.createCellStyle();
			style.setDataFormat(createHelper.createDataFormat().getFormat(format));
			formats.put(format, style);
		}
		
		return style;
	}
	
	/**
	 * Cache the color styles
	 * 
	 */
	private CellStyle getStyleByColor(String foregroundColor, String backgroundColor)
	{
		String color = String.format("%s_%s", foregroundColor, backgroundColor);
		CellStyle style = formats.get(color);
		if (style == null)
		{
			style = wb.createCellStyle();
			if (foregroundColor != null)
			{
				Font font = wb.createFont();
				font.setColor(IndexedColors.valueOf(foregroundColor).getIndex());
				style.setFont(font);
			}
			if (backgroundColor != null)
			{
				style.setFillForegroundColor(IndexedColors.valueOf(backgroundColor).getIndex());
				style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			}
			formats.put(color, style);
		}
		
		return style;
	}

}
