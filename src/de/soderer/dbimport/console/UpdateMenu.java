package de.soderer.dbimport.console;

import de.soderer.dbimport.DbImport;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

public class UpdateMenu extends ConsoleMenu {
	public UpdateMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Update application");
	}

	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");
		System.out.println();
		return -4;
	}
}
