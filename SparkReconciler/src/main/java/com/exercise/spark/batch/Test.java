package com.exercise.spark.batch;

import java.util.Calendar;

import org.apache.commons.lang.time.DateUtils;

public class Test {
	

	public static void main(String[] args) {
		Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);
		System.out.println(cal.getTime().getTime());
	}

}
