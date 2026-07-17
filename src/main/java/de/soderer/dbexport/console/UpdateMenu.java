package de.soderer.dbexport.console;

import de.soderer.dbexport.DbExport;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

public class UpdateMenu extends ConsoleMenu {
	public UpdateMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Update application");
	}

	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");
		System.out.println();
		return -3;
	}
}
