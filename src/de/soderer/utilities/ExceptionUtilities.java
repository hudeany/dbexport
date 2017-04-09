package de.soderer.utilities;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtilities {

	public static String toString(Throwable t) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		printWriter.println(t.getClass().getName());
		printWriter.println(t.getMessage());
		printWriter.println(ExceptionUtilities.getStackTrace(t));
		return stringWriter.toString();
	}

	public static String getStackTrace(Throwable t) {
		StringWriter stringWriter = new StringWriter();
		t.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

	public static String throwableToString(Throwable throwable) {
		return throwableToString(throwable, 5);
	}

	public static String throwableToString(Throwable throwable, int maxLevel) {
		StringBuilder returnBuilder = new StringBuilder(throwable.getMessage() + "\n" + Utilities.stacktraceToString(throwable.getStackTrace()));
		int level = 0;
		Throwable previousSubThrowable = throwable;
		Throwable subThrowable = throwable.getCause();
		while ((maxLevel < 0 || level < maxLevel) && level <= 100 && subThrowable != null && previousSubThrowable != subThrowable) {
			returnBuilder.append("\n\n" + subThrowable.getMessage() + "\n" + Utilities.stacktraceToString(subThrowable.getStackTrace()));
			level++;
			subThrowable = subThrowable.getCause();
		}
		if (level == maxLevel) {
			returnBuilder.append("\n\n... cut after level " + maxLevel + " ...");
		}
		return returnBuilder.toString();
	}
}
