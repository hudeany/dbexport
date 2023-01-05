package de.soderer.utilities;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ClassUtilities {
	public static Constructor<?> getConstructor(final Class<?> clazz) throws NoSuchMethodException, SecurityException {
		return clazz.getDeclaredConstructor();
	}

	public static List<Field> getAllFields(final Class<?> clazz) {
		final List<Field> fields = new ArrayList<>();

		for (final Field field : clazz.getDeclaredFields()) {
			fields.add(field);
		}

		if (clazz.getSuperclass() != null) {
			fields.addAll(getAllFields(clazz.getSuperclass()));
		}

		return fields;
	}

	public static Field getField(final Class<?> clazz, final String fieldName) throws NoSuchFieldException {
		try {
			return clazz.getDeclaredField(fieldName);
		} catch (final NoSuchFieldException e) {
			if (clazz.getSuperclass() != null) {
				return getField(clazz.getSuperclass(), fieldName);
			} else {
				throw e;
			}
		} catch (final SecurityException e) {
			throw e;
		}
	}
}
