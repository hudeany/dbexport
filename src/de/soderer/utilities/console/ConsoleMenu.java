package de.soderer.utilities.console;

import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;

public class ConsoleMenu {
	private final String mainTitle;
	private final String menuTitle;
	private final ConsoleMenu parentMenu;
	private final List<ConsoleMenu> subMenus = new ArrayList<>();

	private final List<String> messages = new ArrayList<>();
	private final List<String> warnings = new ArrayList<>();
	private final List<String> errors = new ArrayList<>();

	public ConsoleMenu(final String mainTitle) {
		this.mainTitle = mainTitle;
		menuTitle = "Main menu";
		parentMenu = null;
	}

	public ConsoleMenu(final ConsoleMenu parentMenu, final String menuTitle) throws Exception {
		if (parentMenu != null) {
			parentMenu.addSubMenu(this);
		} else {
			throw new Exception("Invalid empty parent menu");
		}
		this.parentMenu = parentMenu;
		mainTitle = parentMenu.getMainTitle();
		this.menuTitle = menuTitle;
	}

	public String getMainTitle() {
		return mainTitle;
	}

	public ConsoleMenu getParentMenu() {
		return parentMenu;
	}

	public String getTitle() {
		return menuTitle;
	}

	public List<String> getMessages() {
		return messages;
	}

	public List<String> getWarnings() {
		return warnings;
	}

	public List<String> getErrors() {
		return errors;
	}

	private ConsoleMenu addSubMenu(final ConsoleMenu subMenu) {
		subMenus.add(subMenu);
		return this;
	}

	public int show() throws Exception {
		while (true) {
			ConsoleUtilities.clearScreen();

			ConsoleUtilities.printBoxed(mainTitle);
			System.out.println();

			System.out.println(menuTitle);
			System.out.println();
			printMessages();

			String defaultMenuTitle;
			if (parentMenu == null) {
				defaultMenuTitle = "Quit";
			} else {
				defaultMenuTitle = "Back to " + parentMenu.getTitle();
			}

			System.out.println("Please choose (Blank => " + defaultMenuTitle + "):");

			int subMenuIndex = 0;
			for (final ConsoleMenu subMenu : subMenus) {
				subMenuIndex++;
				System.out.println(" " + (subMenuIndex < 10 ? " " : "") + subMenuIndex + ") " + subMenu.getTitle());
			}

			System.out.println("  0) " + defaultMenuTitle);

			String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
			choice = choice == null ? "" : choice.trim();

			if (Utilities.isBlank(choice)) {
				return 0;
			} else if ("q".equalsIgnoreCase(choice) || "quit".equalsIgnoreCase(choice) || "exit".equalsIgnoreCase(choice)) {
				ConsoleUtilities.clearScreen();
				System.exit(0);
			} else {
				try {
					final int choiceInt = Integer.parseInt(choice);

					if (choiceInt == 0) {
						return 0;
					} else if (1 <= choiceInt && choiceInt <= subMenus.size()) {
						final int returnValue = subMenus.get(choiceInt - 1).show();
						if (returnValue < 0) {
							return returnValue;
						}
					} else {
						errors.add("Invalid input: " + choice);
					}
				} catch (@SuppressWarnings("unused") final Exception e) {
					errors.add("Invalid input: " + choice);
				}
			}
		}
	}

	protected void printMessages() {
		if (errors.size() > 0) {
			for (final String error : errors) {
				System.out.println(ConsoleUtilities.getAnsiColoredText("Error: " + error, TextColor.Light_red));
				System.out.println();
			}
			errors.clear();
		}

		if (warnings.size() > 0) {
			for (final String warning : warnings) {
				System.out.println(ConsoleUtilities.getAnsiColoredText("Warning: " + warning, TextColor.Yellow));
			}
			System.out.println();
			messages.clear();
		}

		if (messages.size() > 0) {
			for (final String message : messages) {
				System.out.println(ConsoleUtilities.getAnsiColoredText(message, TextColor.Green));
			}
			System.out.println();
			messages.clear();
		}
	}

	protected static String askForSelection(final String message, final List<String> selectionValues) throws Exception {
		System.out.println();
		System.out.println(message + " (Blank => Cancel)");
		int selectionIndex = 0;
		for (final String selectionValue : selectionValues) {
			selectionIndex++;
			System.out.println(" " + Utilities.leftPad(Integer.toString(selectionIndex), 2) + ") " + selectionValue);
		}
		String choice = new SimpleConsoleInput().setAutoCompletionStrings(selectionValues).setPrompt(" > ").readInput();
		choice = choice == null ? "" : choice.trim();
		if (Utilities.isBlank(choice)) {
			return null;
		} else if (!NumberUtilities.isNumber(choice)) {
			if (selectionValues.contains(choice)) {
				return choice;
			} else {
				throw new Exception("Invalid selection: " + choice);
			}
		} else {
			final int choiseInt = Integer.parseInt(choice);
			if (choiseInt < 1 || choiseInt > selectionValues.size()) {
				throw new Exception("Invalid selection: " + choice);
			} else {
				final String selectionItemString = selectionValues.get(choiseInt - 1);
				System.out.println(selectionItemString);
				return selectionItemString;
			}
		}
	}
}
