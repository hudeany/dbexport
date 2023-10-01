package de.soderer.dbimport.utilities.jarinjarloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class JarInJarLoader {
	public static final String SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR = "process.jar";

	private static class ManifestInfo {
		String mainClass;
		String[] classPath;
	}

	public static void main(final String[] args) throws Exception {
		jarInJarLoaderStart(args);
	}

	protected static void jarInJarLoaderStart(final String[] args) throws IOException, MalformedURLException,
	InvocationTargetException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
		// Fill an environment variable with the path of the executed jar file
		try {
			final String currentJarUrlPath = JarInJarLoader.class.getResource(JarInJarLoader.class.getSimpleName() + ".class").toString();
			if (currentJarUrlPath != null && currentJarUrlPath.length() > 0) {
				final String jarFilePath = currentJarUrlPath.substring(0, currentJarUrlPath.lastIndexOf("!")).replaceFirst("jar:file:", "");
				final File jarFile = new File(jarFilePath);
				if (jarFile.exists()) {
					System.getProperties().put(SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR, jarFile.getAbsolutePath());
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
		}

		final ManifestInfo manifestInfo = getManifestInfo();
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		URL.setURLStreamHandlerFactory(new JarInJarURLStreamHandlerFactory(classLoader));
		final URL[] classPathUrls = new URL[manifestInfo.classPath.length];
		for (int i = 0; i < manifestInfo.classPath.length; i++) {
			final String classPath = manifestInfo.classPath[i];
			if (classPath.endsWith("/")) {
				classPathUrls[i] = new URL("rsrc:" + classPath);
			} else {
				classPathUrls[i] = new URL("jar:rsrc:" + classPath + "!/");
			}
		}
		final URLClassLoader jceClassLoader = new URLClassLoader(classPathUrls, getParentClassLoader());
		Thread.currentThread().setContextClassLoader(jceClassLoader);
		final Class<?> mainClass = Class.forName(manifestInfo.mainClass, true, jceClassLoader);
		final Method mainMethod = mainClass.getMethod("main", new Class[] { args.getClass() });
		mainMethod.invoke((Object) null, new Object[] { args });
	}

	private static ClassLoader getParentClassLoader() throws InvocationTargetException, IllegalAccessException {
		try {
			// We use reflection here because the method ClassLoader.getPlatformClassLoader() is only present starting from Java 9
			final Method platformClassLoader = ClassLoader.class.getMethod("getPlatformClassLoader", (Class[]) null);
			return (ClassLoader) platformClassLoader.invoke(null, (Object[]) null);
		} catch (@SuppressWarnings("unused") final NoSuchMethodException e) {
			// This is a safe value to be used on Java 8 and previous versions
			return null;
		}
	}

	private static ManifestInfo getManifestInfo() throws IOException {
		final Enumeration<URL> manifestFileUrls = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
		while (manifestFileUrls.hasMoreElements()) {
			final URL manifestFileUrl = manifestFileUrls.nextElement();
			try (InputStream inputStream = manifestFileUrl.openStream()) {
				if (inputStream != null) {
					final ManifestInfo manifestInfo = new ManifestInfo();
					final Attributes mainAttributes = new Manifest(inputStream).getMainAttributes();
					manifestInfo.mainClass = mainAttributes.getValue("Rsrc-Main-Class");
					String rsrcClassPath = mainAttributes.getValue("Rsrc-Class-Path");
					if (rsrcClassPath == null) {
						// find all jar files included in the jar and add them to the classpath
						rsrcClassPath = "./";
						String jarFilePath = manifestFileUrl.getFile().substring(0, manifestFileUrl.getPath().indexOf("!/META-INF/MANIFEST.MF"));
						if (jarFilePath.startsWith("file:")) {
							jarFilePath = jarFilePath.substring(5);
						}
						boolean swtFound = false;
						boolean swtLoaded = false;
						final String osName = System.getProperty("os.name").toLowerCase();
						final String osArch = System.getProperty("os.arch").toLowerCase();
						try {
							for (final String entryName : getZipFileEntries(new File(jarFilePath))) {
								if (endsWithIgnoreCase(entryName, ".jar")) {
									if (entryName.startsWith("swt")) {
										swtFound = true;
										if (osArch.contains("64") && osName.contains("win") && entryName.contains("win32") && entryName.contains("x86_64")) {
											rsrcClassPath += " " + entryName;
											swtLoaded = true;
										} else if (osArch.contains("64") && (osName.contains("linux") || osName.contains("nix")) && entryName.contains("linux") && entryName.contains("gtk") && entryName.contains("x86_64")) {
											rsrcClassPath += " " + entryName;
											swtLoaded = true;
										}
									} else {
										rsrcClassPath += " " + entryName;
									}
								}
							}
						} catch (final Exception e) {
							e.printStackTrace();
						}
						if (swtFound && !swtLoaded) {
							throw new Exception("Unsupported OS name or architecture for this SWT application: " + osName + " / " + osArch);
						}
					}
					manifestInfo.classPath = rsrcClassPath.split(" ");
					if (isNotBlank(manifestInfo.mainClass)) {
						return manifestInfo;
					}
				}
			} catch (@SuppressWarnings("unused") final Exception e) {
				// Skip invalid manifest file
			}
		}
		System.err.println("Missing attributes for JarInJarLoader in Manifest (Rsrc-Main-Class, Rsrc-Class-Path)");
		return null;
	}

	private static boolean isBlank(final String value) {
		return value == null || value.length() == 0 || value.trim().length() == 0;
	}

	private static boolean isNotBlank(final String value) {
		return !isBlank(value);
	}

	private static String[] getZipFileEntries(final File file) throws ZipException, IOException {
		try (ZipFile zipFile = new ZipFile(file)) {
			final Enumeration<? extends ZipEntry> entries = zipFile.entries();
			final List<String> entryList = new ArrayList<>();
			while (entries.hasMoreElements()) {
				final ZipEntry entry = entries.nextElement();
				entryList.add(entry.getName());
			}
			return entryList.toArray(new String[0]);
		}
	}

	private static boolean endsWithIgnoreCase(final String data, final String suffix) {
		if (data == suffix) {
			// both null or same object
			return true;
		} else if (data == null) {
			// data is null but suffix is not
			return false;
		} else if (suffix == null) {
			// suffix is null but data is not
			return true;
		} else if (data.toLowerCase().endsWith(suffix.toLowerCase())) {
			// both are set, so ignore the case for standard endsWith-method
			return true;
		} else {
			// anything else
			return false;
		}
	}
}
