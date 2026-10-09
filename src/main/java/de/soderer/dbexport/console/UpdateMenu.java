package de.soderer.dbexport.console;

import de.soderer.dbexport.DbExport;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

/**
 * Console menu entry, which starts the application update.
 */
public class UpdateMenu extends ConsoleMenu {
	/**
	 * Creates the menu.
	 *
	 * @param parentMenu the parent menu
	 * @throws Exception if the menu cannot be created
	 */
	public UpdateMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Update application");
	}

	/**
	 * Requests the application update.
	 *
	 * @return -3 to start the application update
	 * @throws Exception if the console output fails
	 */
	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");
		System.out.println();
		return -3;
	}
}
