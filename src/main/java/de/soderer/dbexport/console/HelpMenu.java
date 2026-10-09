package de.soderer.dbexport.console;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import de.soderer.dbexport.DbExport;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

/**
 * Console menu entry, which prints the help text and exits the application.
 */
public class HelpMenu extends ConsoleMenu {
	private static final boolean keyCodeDebugMode = false;

	/**
	 * Creates the menu.
	 *
	 * @param parentMenu the parent menu
	 * @throws Exception if the menu cannot be created
	 */
	public HelpMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Print help text");
	}

	/**
	 * Prints the help text and exits the application.
	 *
	 * @return 0 (not reached, the application is exited)
	 * @throws Exception if the console output fails
	 */
	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		try (InputStream helpInputStream = DbExport.class.getResourceAsStream(DbExport.HELP_RESOURCE_FILE)) {
			System.out.println("DbExport (by Andreas Soderer, mail: dbexport@soderer.de)\n"
					+ "VERSION: " + DbExport.VERSION.toString() + DbExport.getBuildTimeText() + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8));
		} catch (@SuppressWarnings("unused") final Exception e) {
			System.out.println("Help info is missing");
		}

		if (keyCodeDebugMode) {
			System.out.println("Press ENTER to close");
			int lastkey = -1;
			while ((lastkey = ConsoleUtilities.readNextRawKey()) != ConsoleUtilities.KeyCode_Enter) {
				System.out.println(lastkey + "   (" + (char) lastkey + ")");
			}
		}

		System.exit(0);
		return 0;
	}
}
