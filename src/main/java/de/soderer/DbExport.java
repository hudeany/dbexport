package de.soderer;

import de.soderer.dbexport.utilities.jarinjarloader.JarInJarLoader;

/**
 * Launcher class of the executable DbExport jar file.
 *
 * <p>
 * Sets up a class loader for the library jars embedded in the running jar file and then starts the
 * actual main class named in the manifest (see {@link JarInJarLoader}).
 * </p>
 */
public class DbExport extends JarInJarLoader {
	private DbExport() {
		// Only the static main method is used
	}

	/**
	 * Starts the application.
	 *
	 * @param args command line arguments, passed on unchanged to the actual main class
	 * @throws Exception if the embedded libraries or the main class cannot be loaded or started
	 */
	public static void main(final String[] args) throws Exception {
		jarInJarLoaderStart(args);
	}
}
