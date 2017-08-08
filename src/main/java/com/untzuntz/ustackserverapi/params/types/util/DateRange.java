package com.untzuntz.ustackserverapi.params.types.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

/**
 * Process a date range from a string
 * 
 * Examples:
 * 
 * Last X:
 * 		last 5 days
 * 		last 10 months
 * 		last 3 weeks
 * 		last 1 years
 * Range:
 * 		20121201=>20121231
 * Before:
 * 		<20121201
 * After:
 * 		>20121201
 * One day only:
 * 		20121201	(processed as 20121201 00:00:00 to 20121201 23:59:59
 * 
 * @author jdanner
 *
 */
public class DateRange {

	static 		Logger           	logger                  = Logger.getLogger(DateRange.class);	
	public static final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	static 
	{
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		df.setLenient(false);
	}
	
	public DateRange(String range) throws ParseException {
		process(range);
	}
	
	private void process(String data) throws ParseException
	{
		if (data == null)
		{
			start = null;
			end = null;
			return;
		}
		
		if (data.toLowerCase().startsWith("last "))
		{
			int amount = 0;
			int field = Calendar.DATE;
			if (data.toLowerCase().endsWith(" days"))
			{
				amount = Integer.valueOf( data.substring(5, data.length() - 5) );
				field = Calendar.DATE;
			}
			else if (data.toLowerCase().endsWith(" weeks"))
			{
				amount = Integer.valueOf( data.substring(5, data.length() - 6) ) * 7;
				field = Calendar.DATE;
			}
			else if (data.toLowerCase().endsWith(" months"))
			{
				amount = Integer.valueOf( data.substring(5, data.length() - 7) );
				field = Calendar.MONTH;
			}
			else if (data.toLowerCase().endsWith(" years"))
			{
				amount = Integer.valueOf( data.substring(5, data.length() - 6) );
				field = Calendar.YEAR;
			}
			else if (data.toLowerCase().endsWith(" year"))
			{
				amount = Integer.valueOf( data.substring(5, data.length() - 5) );
				field = Calendar.YEAR;
			}
			else
				throw new ParseException("Cannot process 'last X timeframe'", 0);
			
			Calendar now = Calendar.getInstance();
			now.add(field, -1 * amount);
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.endsWith(" days"))
		{
			Calendar now = Calendar.getInstance();
			now.add(Calendar.DATE, Integer.valueOf( data.substring(0, data.length() - 5) ));
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.endsWith(" weeks"))
		{
			Calendar now = Calendar.getInstance();
			now.add(Calendar.DATE, Integer.valueOf( data.substring(0, data.length() - 6) ) * 7);
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.endsWith(" months"))
		{
			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, Integer.valueOf( data.substring(0, data.length() - 7) ));
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.endsWith(" years"))
		{
			Calendar now = Calendar.getInstance();
			now.add(Calendar.YEAR, Integer.valueOf( data.substring(0, data.length() - 6) ));
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.endsWith(" year"))
		{
			Calendar now = Calendar.getInstance();
			now.add(Calendar.YEAR, Integer.valueOf( data.substring(0, data.length() - 5) ));
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			start = now.getTime();
		}
		else if (data.indexOf("=>") > -1)
		{
			int rangeIdx = data.indexOf("=>");
			
			// range
			start = getDate(data.substring(0, rangeIdx), true);
			end = getDate(data.substring(rangeIdx + 2), false);
		}
		else if (data.startsWith(">"))
		{
			// gt
			start = getDate(data.substring(1), false);
		}
		else if (data.startsWith("<"))
		{
			// lt
			end = getDate(data.substring(1), true);
		}
		else
		{
			// set range to low and high of past the provided value
			// ex: provided [20121228] start = [20121228000000] end = [20121228235959]
			start = getDate(data, true);
			end = getDate(data, false);
		}

	}
	
	/**
	 * Appends proper fields to the provided date string
	 * 
	 * @param date
	 * @param lowMode
	 * @return
	 */
	public static String fixDate(String date, boolean lowMode) throws ParseException
	{
		if (date.length() < 17)
		{
			int dateLen = date.length();
			
			if (dateLen < 4 || dateLen % 2 == 1)
				throw new ParseException("Cannot process date - invalid length! (must be at least 4 chars and even)", dateLen);

			StringBuffer fix = new StringBuffer();
			fix.append(date);
			
			if (lowMode)
			{
				if (fix.length() == 4)
					fix.append("01");
				if (fix.length() == 6)
					fix.append("01");

				while (fix.length() < 17) {
					fix.append("0");
				}
			}
			else
			{
				if (fix.length() == 4)
					fix.append("12");
				if (fix.length() == 6)
				{
					int month = Integer.valueOf(fix.substring(4, 6));
					int append = 31;
					if (month % 2 == 0)
						append = 30;
					if (month == 2)
						append = 28;
					
					fix.append(append);
				}
				if (fix.length() == 8)
					fix.append("23");
				if (fix.length() == 10)
					fix.append("59");
				if (fix.length() == 12)
					fix.append("59");
				if (fix.length() == 14)
					fix.append("999");
			}
			
			date = fix.toString();
		}

		return date;
	}
	
	public String toString() {
		
		StringBuffer ret = new StringBuffer();
		
		if (start != null && end != null)
			ret.append(df.format(start)).append("->").append(df.format(end));
		else if (start != null)
			ret.append(">").append(df.format(start));
		else if (end != null)
			ret.append("<").append(df.format(end));
		
		return ret.toString();
		
	}
	
	/**
	 * Returns a date based on the format of yyyyMMddHHmmss
	 * 
	 * if lowMode = true then we fill in fields to low values (beginning of the month/day/hour/min/sec)
	 * if lowMode = false then we fill in fields to high values (end of the month/day/hour/min/sec)
	 * 
	 * @param date
	 * @param lowMode
	 * @return
	 */
	public Date getDate(String date, boolean lowMode) throws ParseException
	{
		// yyyyMMddHHmmss
		
		if (date == null)
			return null;
		
		date = fixDate(date, lowMode);
		
		return df.parse(date);
	}

	
	public Date start;
	public Date end;
	
	public Date getStart() {
		return start;
	}
	public Date getEnd() {
		return end;
	}
	

}
