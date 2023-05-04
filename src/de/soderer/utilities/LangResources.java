package de.soderer.utilities;

import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class LangResources {
	private static PropertyResourceBundle propertyResourceBundle;

	public static void enforceDefaultLocale() {
		try {
			propertyResourceBundle = (PropertyResourceBundle) ResourceBundle.getBundle("LanguageProperties", Locale.ROOT);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	public static String get(final String resourceKey, final Object... arguments) {
		if (propertyResourceBundle == null) {
			try {
				propertyResourceBundle = (PropertyResourceBundle) ResourceBundle.getBundle("LanguageProperties");
			} catch (final Exception e) {
				e.printStackTrace();
				return "Missing resourceBundle: " + resourceKey + (arguments != null && arguments.length > 0 ? " " + Utilities.join(arguments, ", ") : "");
			}
		}

		if (propertyResourceBundle != null) {
			if (propertyResourceBundle.containsKey(resourceKey)) {
				final String pattern = propertyResourceBundle.getString(resourceKey);
				return format(pattern, arguments);
			} else {
				return "Missing resourceKey: " + resourceKey + (arguments != null && arguments.length > 0 ? " " + Utilities.join(arguments, ", ") : "");
			}
		} else {
			return "Missing resourceBundle: " + resourceKey + (arguments != null && arguments.length > 0 ? " " + Utilities.join(arguments, ", ") : "");
		}
	}

	private static String format(String pattern, final Object[] arguments) {
		if (arguments != null) {
			for (int i = 0; i < arguments.length; i++) {
				final Object item = arguments [i];
				pattern = pattern.replace("{" + i + "}", item == null ? "" : item.toString());
			}
		}
		return pattern;
	}

	public static boolean existsKey(final String resourceKey) {
		if (propertyResourceBundle == null) {
			try {
				propertyResourceBundle = (PropertyResourceBundle) ResourceBundle.getBundle("LanguageProperties");
			} catch (@SuppressWarnings("unused") final Exception e) {
				// Do nothing
			}
		}

		if (propertyResourceBundle != null) {
			return propertyResourceBundle.containsKey(resourceKey);
		} else {
			return false;
		}
	}
}
