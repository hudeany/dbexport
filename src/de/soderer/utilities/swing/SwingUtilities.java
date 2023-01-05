package de.soderer.utilities.swing;

import java.awt.Toolkit;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;

import javax.swing.UIManager;

import de.soderer.utilities.SystemUtilities;

public class SwingUtilities {
	/**
	 * Prevent Exceptions of type:
	 *     Exception in thread "AWT-EventQueue-0" java.lang.NoClassDefFoundError
	 * when calling Classes within AWT Threads when jar-in-jar-loader is used
	 * @param <T>
	 * @param clazz
	 */
	public static void forceConstructorInit(final Class<?> clazz) {
		try {
			final Class<?> initClass = Class.forName(clazz.getName(), true, clazz.getClassLoader());
			final Constructor<?>[] constructors = initClass.getConstructors();
			if (constructors.length > 0) {
				final Constructor<?> constructor = constructors[0];
				final Object[] parameters = new Object[constructor.getParameters().length];
				int i = 0;
				for (final Parameter parameter : constructor.getParameters()) {
					if (parameter.getType() == boolean.class) {
						parameters[i] = false;
					} else if (parameter.getType() == int.class) {
						parameters[i] = 0;
					} else if (parameter.getType() == long.class) {
						parameters[i] = 0;
					} else if (parameter.getType() == float.class) {
						parameters[i] = 0;
					} else if (parameter.getType() == double.class) {
						parameters[i] = 0;
					} else if (parameter.getType() == char.class) {
						parameters[i] = 'x';
					} else {
						parameters[i] = null;
					}
					i++;
				}
				constructor.newInstance(parameters);
			}
		} catch (@SuppressWarnings("unused") final InvocationTargetException e) {
			// Something went wrong inside the constructor because of the dummy values.
			// This doesen't matter, the classloader loaded the class and constructors anyway
			// do nothing
		} catch (final Exception e) {
			System.out.println("forceConstructorInit failed for class: " + clazz.getName());
			e.printStackTrace();
		}
	}

	/**
	 * Set WM_CLASS for Linux Gnome window manager to allow application to be pinned to Linux Gnome applications dock
	 * Needs java runtime option "--add-opens=java.desktop/sun.awt.X11=ALL-UNNAMED"
	 */
	public static void setAwtWmClass(final String applicationName) {
		if (SystemUtilities.isLinuxSystem()) {
			try {
				final Toolkit toolkit = Toolkit.getDefaultToolkit();
				final Field awtAppClassNameField = toolkit.getClass().getDeclaredField("awtAppClassName");
				awtAppClassNameField.setAccessible(true);
				awtAppClassNameField.set(toolkit, applicationName);
			} catch (final Exception e) {
				System.err.println("Cannot set AWT WM_CLASS: " + e.getMessage());

				System.err.println("Adding open directive");
				final Module mod = SwingUtilities.class.getClassLoader().getUnnamedModule();
				if (mod == Toolkit.class.getModule()) {
					Toolkit.getDefaultToolkit().getClass().getModule().addOpens("sun.awt.X11", mod);
				}

				try {
					final Toolkit toolkit = Toolkit.getDefaultToolkit();
					final Field awtAppClassNameField = toolkit.getClass().getDeclaredField("awtAppClassName");
					awtAppClassNameField.setAccessible(true);
					awtAppClassNameField.set(toolkit, applicationName);
				} catch (final Exception e2) {
					System.err.println("Cannot set AWT WM_CLASS: " + e2.getMessage());
				}
			}
		}
	}

	public static void setSystemLookAndFeel() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}
