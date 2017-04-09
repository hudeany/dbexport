package de.soderer.utilities;

import java.awt.HeadlessException;
import java.io.Console;
import java.util.Date;

public class BasicUpdateableConsoleApplication implements UpdateParent {
	private String applicationName;
	private Version applicationVersion;
	
	public BasicUpdateableConsoleApplication(String applicationName, Version applicationVersion) throws HeadlessException {
		this.applicationName = applicationName;
		this.applicationVersion = applicationVersion;
	}

	@Override
	public boolean askForUpdate(String availableNewVersion) throws Exception {
		Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}
		if (availableNewVersion == null) {
			System.out.println("There is no newer version available for " + applicationName + "\nThe current local version is " + applicationVersion.toString());
			System.out.println();
			return false;
		} else {
			System.out.println("New version " + availableNewVersion + " is available.\nCurrent version is " + applicationVersion.toString() + ".");
			String input = console.readLine("Install update? (yN): ");
			System.out.println();
			return input != null && (input.toLowerCase().startsWith("y") || input.toLowerCase().startsWith("j"));
		}
	}

	@Override
	public Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword) throws Exception {
		Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}

		String userName = null;
		char[] password = null;
		
		if (aquireUsername && aquirePassword) {
			userName = console.readLine("Please enter username: ");
			password = console.readPassword("Please enter password: ");
		} else if (aquireUsername) {
			userName = console.readLine("Please enter username: ");
		} else if (aquirePassword) {
			password = console.readPassword("Please enter password: ");
		}
		
		if (Utilities.isBlank(userName) && Utilities.isBlank(password)) {
			return null;
		} else {
			return new Credentials(userName, password);
		}
	}

	@Override
	public void showUpdateError(String errorText) {
		System.err.println(errorText);
	}

	@Override
	public void showUpdateProgress(Date itemStart, long itemsToDo, long itemsDone) {
		System.out.print("\r" + Utilities.getConsoleProgressString(80, itemStart, itemsToDo, itemsDone));
	}

	@Override
	public void showUpdateDone() {
		System.out.println();
		System.out.println("Restarting after update");
		System.out.println();
	}

	@Override
	public void showUpdateDownloadStart() {
	}

	@Override
	public void showUpdateDownloadEnd() {
		// Do nothing
	}
}
