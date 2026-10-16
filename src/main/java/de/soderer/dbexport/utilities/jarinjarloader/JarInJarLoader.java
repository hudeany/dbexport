package de.soderer.dbexport.utilities.jarinjarloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * Start class for applications packed as a single jar file with their library jars inside (jar in jar).
 * <p>
 * It reads the main class from the manifest attribute "Rsrc-Main-Class" and the embedded libraries from
 * "Rsrc-Class-Path" (separated by blanks, URL encoded like "my%20lib.jar"). Without "Rsrc-Class-Path" all jar files
 * contained in the application jar are used, where of the SWT jars (file name starting with "swt") only the one
 * matching the current OS (Windows, Linux, macOS) and architecture (x86_64, aarch64) is included.
 * Then it starts the main class with a class loader, that loads classes from the embedded jars via the
 * "rsrc:" URL protocol.
 */
public class JarInJarLoader {
	/**
	 * Name of the system property, which is set to the absolute path of the running application jar file.
	 */
	public static final String SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR = "process.jar";

	/**
	 * Constructor for subclasses, which provide their own main method calling {@link #jarInJarLoaderStart(String[])}.
	 */
	protected JarInJarLoader() {
		// Only static methods
	}

	private static class ManifestInfo {
		String mainClass;
		List<URL> classPathUrls = new ArrayList<>();
	}

	/**
	 * Starts the application configured in the manifest.
	 *
	 * @param args command line arguments, which are passed on to the application's main method
	 * @throws Exception if the manifest is incomplete, the platform is not supported, or the application fails to start.
	 *             An exception thrown by the application's main method is passed on unwrapped.
	 */
	public static void main(final String[] args) throws Exception {
		try {
			jarInJarLoaderStart(args);
		} catch (final InvocationTargetException e) {
			// Report the application's own exception instead of the reflection wrapper
			final Throwable cause = e.getCause();
			if (cause instanceof Exception) {
				throw (Exception) cause;
			} else if (cause instanceof Error) {
				throw (Error) cause;
			} else {
				throw e;
			}
		}
	}

	/**
	 * Starts the application configured in the manifest. Can be called by subclasses with their own main method.
	 *
	 * @param args command line arguments, which are passed on to the application's main method
	 * @throws IOException if no manifest with the required attributes is found or the platform is not supported by the embedded SWT jars
	 * @throws MalformedURLException if an embedded class path entry results in an invalid URL
	 * @throws InvocationTargetException if the application's main method throws an exception
	 * @throws IllegalAccessException if the application's main method is not accessible
	 * @throws ClassNotFoundException if the application's main class is not found
	 * @throws NoSuchMethodException if the application's main class has no static main method
	 */
	protected static void jarInJarLoaderStart(final String[] args) throws IOException, MalformedURLException,
	InvocationTargetException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
		// Fill a system property with the path of the executed jar file
		final Path runningJarFile = getRunningJarFile();
		if (runningJarFile != null) {
			System.getProperties().put(SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR, runningJarFile.toAbsolutePath().toString());
		}

		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		registerRsrcProtocol(classLoader);
		final ManifestInfo manifestInfo = getManifestInfo();
		final URLClassLoader jceClassLoader = new URLClassLoader(manifestInfo.classPathUrls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
		Thread.currentThread().setContextClassLoader(jceClassLoader);
		final Class<?> mainClass = Class.forName(manifestInfo.mainClass, true, jceClassLoader);
		final Method mainMethod = mainClass.getMethod("main", String[].class);
		if (!Modifier.isStatic(mainMethod.getModifiers())) {
			throw new NoSuchMethodException("Method main of class " + manifestInfo.mainClass + " is not static");
		}
		mainMethod.invoke((Object) null, new Object[] { args });
	}

	/**
	 * Returns the jar file this class was loaded from.
	 *
	 * @return jar file, or null if the class was not loaded from a jar file (e.g. from a class folder in an IDE)
	 */
	private static Path getRunningJarFile() {
		try {
			final CodeSource codeSource = JarInJarLoader.class.getProtectionDomain().getCodeSource();
			if (codeSource != null && codeSource.getLocation() != null) {
				// URI based conversion handles percent-encoding, Windows drive letters and UNC paths
				final Path codeSourcePath = Paths.get(codeSource.getLocation().toURI());
				if (Files.isRegularFile(codeSourcePath)) {
					return codeSourcePath;
				}
			}
		} catch (final Exception e) {
			System.err.println("Cannot determine path of the running jar file: " + e.getMessage());
		}
		return null;
	}

	/**
	 * Registers the "rsrc:" URL protocol. The URL stream handler factory can only be set once per JVM, so a factory set
	 * before (e.g. by an earlier start within the same JVM) is accepted, as long as it provides the "rsrc:" protocol.
	 */
	private static void registerRsrcProtocol(final ClassLoader classLoader) throws MalformedURLException {
		try {
			URL.setURLStreamHandlerFactory(new JarInJarURLStreamHandlerFactory(classLoader));
		} catch (final Error e) {
			try {
				URI.create("rsrc:test").toURL();
			} catch (@SuppressWarnings("unused") final MalformedURLException | IllegalArgumentException e1) {
				throw new MalformedURLException("Cannot register the URL protocol 'rsrc:', another URLStreamHandlerFactory is already set: " + e.getMessage());
			}
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
					if (isBlank(manifestInfo.mainClass)) {
						continue;
					}

					final String rsrcClassPath = mainAttributes.getValue("Rsrc-Class-Path");
					if (rsrcClassPath != null) {
						// Manifest entries are separated by blanks and must be URL encoded themselves (e.g. "my%20lib.jar")
						for (final String classPathEntry : rsrcClassPath.trim().split("\\s+")) {
							manifestInfo.classPathUrls.add(createClassPathUrl(classPathEntry));
						}
					} else {
						// Find all jar files included in the jar and add them to the classpath
						manifestInfo.classPathUrls.add(createClassPathUrl("./"));
						for (final String entryName : getEmbeddedJarFiles(getJarFileOfUrl(manifestFileUrl))) {
							// Real file names may contain blanks and other characters, which must be encoded for the URL
							manifestInfo.classPathUrls.add(createClassPathUrl(encodeResourcePath(entryName)));
						}
					}
					return manifestInfo;
				}
			} catch (final UnsupportedOperationException e) {
				// Report the unsupported platform instead of the misleading "missing attributes" error below
				throw new IOException(e.getMessage(), e);
			} catch (final MalformedURLException | IllegalArgumentException e) {
				// A broken class path entry must not be reported as "missing attributes" either
				throw new IOException("Invalid entry in Rsrc-Class-Path: " + e.getMessage(), e);
			} catch (@SuppressWarnings("unused") final Exception e) {
				// Skip invalid manifest file
			}
		}
		throw new IOException("Missing attributes for JarInJarLoader in Manifest (Rsrc-Main-Class, Rsrc-Class-Path)");
	}

	/**
	 * Creates the class path URL for an embedded jar file ("jar:rsrc:lib.jar!/") or folder ("rsrc:folder/").
	 *
	 * @param encodedPath URL encoded path inside the application jar
	 */
	private static URL createClassPathUrl(final String encodedPath) throws MalformedURLException {
		if (encodedPath.endsWith("/")) {
			return URI.create("rsrc:" + encodedPath).toURL();
		} else {
			return URI.create("jar:rsrc:" + encodedPath + "!/").toURL();
		}
	}

	/**
	 * URL encodes a path inside the application jar, keeping the "/" separators.
	 */
	private static String encodeResourcePath(final String path) throws URISyntaxException {
		// The multi-argument URI constructor quotes blanks, '%', '#' and other characters not allowed in a URI
		return new URI(null, null, path, null).getRawPath();
	}

	/**
	 * Returns the local jar file containing the resource of a "jar:file:...!/..." URL.
	 */
	private static File getJarFileOfUrl(final URL url) throws IOException, URISyntaxException {
		final URLConnection urlConnection = url.openConnection();
		if (!(urlConnection instanceof JarURLConnection)) {
			throw new IOException("Not a jar URL: " + url);
		}
		// URI based conversion handles percent-encoding, Windows drive letters and UNC paths
		return Paths.get(((JarURLConnection) urlConnection).getJarFileURL().toURI()).toFile();
	}

	/**
	 * Returns the embedded jar files of the application jar. Of the SWT jars only the one matching the current OS and
	 * architecture is included.
	 *
	 * @throws UnsupportedOperationException if the application jar contains SWT jars, but none for the current platform
	 */
	private static List<String> getEmbeddedJarFiles(final File applicationJarFile) throws IOException {
		final String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
		final String osArch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
		final String[] swtPlatformTokens = getSwtPlatformTokens(osName, osArch);

		final List<String> embeddedJarFiles = new ArrayList<>();
		boolean swtFound = false;
		boolean swtLoaded = false;
		for (final String entryName : getZipFileEntries(applicationJarFile)) {
			if (endsWithIgnoreCase(entryName, ".jar")) {
				// Only the file name decides, the SWT jars may also be located in a sub folder like "lib/"
				final String fileName = entryName.substring(entryName.lastIndexOf('/') + 1).toLowerCase(Locale.ROOT);
				if (fileName.startsWith("swt")) {
					swtFound = true;
					if (swtPlatformTokens != null && containsAll(fileName, swtPlatformTokens)) {
						embeddedJarFiles.add(entryName);
						swtLoaded = true;
					}
				} else {
					embeddedJarFiles.add(entryName);
				}
			}
		}
		if (swtFound && !swtLoaded) {
			throw new UnsupportedOperationException("Unsupported OS name or architecture for this SWT application: " + osName + " / " + osArch);
		}
		return embeddedJarFiles;
	}

	/**
	 * Returns the parts, which the name of the SWT jar for the given platform must contain, e.g. "win32" and "x86_64".
	 *
	 * @return name parts, or null for an unsupported platform
	 */
	static String[] getSwtPlatformTokens(final String osName, final String osArch) {
		final String archToken;
		if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
			archToken = "x86_64";
		} else if ("aarch64".equals(osArch) || "arm64".equals(osArch)) {
			// Must not fall back to the x86_64 SWT jar, whose native libraries cannot be loaded on ARM
			archToken = "aarch64";
		} else {
			return null;
		}

		if (osName.contains("win")) {
			return new String[] { "win32", archToken };
		} else if (osName.contains("linux")) {
			return new String[] { "linux", "gtk", archToken };
		} else if (osName.contains("mac") || osName.contains("darwin")) {
			return new String[] { "macosx", archToken };
		} else {
			return null;
		}
	}

	private static boolean containsAll(final String value, final String[] tokens) {
		for (final String token : tokens) {
			if (!value.contains(token)) {
				return false;
			}
		}
		return true;
	}

	private static boolean isBlank(final String value) {
		return value == null || value.trim().length() == 0;
	}

	private static List<String> getZipFileEntries(final File file) throws ZipException, IOException {
		try (ZipFile zipFile = new ZipFile(file)) {
			final Enumeration<? extends ZipEntry> entries = zipFile.entries();
			final List<String> entryList = new ArrayList<>();
			while (entries.hasMoreElements()) {
				final ZipEntry entry = entries.nextElement();
				entryList.add(entry.getName());
			}
			return entryList;
		}
	}

	private static boolean endsWithIgnoreCase(final String data, final String suffix) {
		if (data == null || suffix == null) {
			return data == suffix;
		} else {
			return data.toLowerCase(Locale.ROOT).endsWith(suffix.toLowerCase(Locale.ROOT));
		}
	}
}
