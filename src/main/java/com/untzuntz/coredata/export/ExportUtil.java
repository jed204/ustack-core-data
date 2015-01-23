package com.untzuntz.coredata.export;

import java.io.IOException;
import java.util.List;

public class ExportUtil {

	public static final byte[] QUOTE = "\"".getBytes();
	public static final byte[] COMMA = ",".getBytes();
	public static final byte[] NEWLINE = "\n".getBytes();

	/**
	 * Closes a stream, handling all exceptions
	 * @param in
	 */
	public static void close(java.io.Closeable in) 
	{
		if (in == null)
			return;
		
		try {
			in.close();
		} catch (Exception e) { 
			// do nothing
		}
	}

	/**
	 * Returns the maximum number of lines for a provided value
	 * @param fields
	 * @param data
	 * @return
	 */
	public static int getMaxLines(List<ExportFieldFormat> fields, Object data)
	{
		int maxLines = 1;
		int fieldCount = fields.size();
		for (int i = 0; i < fieldCount; i++)
		{
			ExportFieldFormat field = fields.get(i);
			field.calculateLineCount(data);
			int lc = field.getLineCount();
			if (lc > maxLines)
				maxLines = lc;
		}
		
		return maxLines;
	}

	/**
	 * Outputs to one or more lines based on the object provided
	 * 
	 * @param out
	 * @param fields
	 * @param data
	 * @throws IOException 
	 */
	public static void writeMultiLine(OutputCallback out, List<ExportFieldFormat> fields, Object data) throws Exception
	{
		int maxLines = ExportUtil.getMaxLines(fields, data);
		int fieldCount = fields.size();
		for (int line = 0; line < maxLines; line++)
		{
			out.startLine();
			
			for (int fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++)
			{
				ExportFieldFormat eff = fields.get(fieldIdx);
				int outputLine = line;
				int lineCnt = eff.getLineCount();
				if (lineCnt < line)
				{
					// re-index to proper outputline for this field
					outputLine = outputLine % lineCnt;
				}
				
				out.writeField(eff.getFieldValue(data, outputLine), eff);
				
				if ((fieldIdx + 1) < fieldCount)
					out.nextField();
			}

			out.nextLine();
		}
		

	}
	
}
