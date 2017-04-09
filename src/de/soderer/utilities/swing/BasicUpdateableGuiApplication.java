package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.HeadlessException;
import java.util.Date;

import javax.swing.JFrame;

import de.soderer.utilities.Credentials;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.UpdateParent;
import de.soderer.utilities.Version;

public class BasicUpdateableGuiApplication extends JFrame implements UpdateParent {
	private static final long serialVersionUID = 1869492889654406822L;
	
	private String applicationName;
	private Version applicationVersion;
	
	private SimpleProgressDialog<?> updateProgressDialog;
	
	public BasicUpdateableGuiApplication(String applicationName, Version applicationVersion) throws HeadlessException {
		super(applicationName + " (Version " + applicationVersion.toString() + ")");
		
		this.applicationName = applicationName;
		this.applicationVersion = applicationVersion;
	}

	@Override
	public Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword) throws Exception {
		CredentialsDialog credentialsDialog = new CredentialsDialog(this, "Update " + applicationName, text, aquireUsername, aquirePassword);
		credentialsDialog.setVisible(true);
		return credentialsDialog.getCredentials();
	}

	@Override
	public void showUpdateError(String errorText) {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
		
		new TextDialog(this, "Update Error", errorText, LangResources.get("close"), false, Color.PINK).setVisible(true);
	}

	@Override
	public void showUpdateProgress(Date itemStart, long itemsToDo, long itemsDone) {
		if (updateProgressDialog != null) {
			updateProgressDialog.showProgress(itemStart, itemsToDo, itemsDone);
		}
	}

	@Override
	public void showUpdateDone() {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
		
		new QuestionDialog(this, "Update Error", "Updated application may be restarted", "Restart now").setVisible(true);
	}

	@Override
	public boolean askForUpdate(String availableNewVersion) throws Exception {
		if (availableNewVersion == null) {
			TextDialog textDialog = new TextDialog(this, "Update", "There is no newer version available for " + applicationName + "\nThe current local version is " + applicationVersion.toString(), LangResources.get("close"));
			textDialog.setVisible(true);
			return false;
		} else {
			QuestionDialog questionDialog = new QuestionDialog(this, "Update", "New version " + availableNewVersion + " is available.\nCurrent version is " + applicationVersion.toString() + ".\nInstall update?", "Yes", "No");
			questionDialog.setVisible(true);
			return questionDialog.getAnswerButtonIndex() == 0;
		}
	}

	@Override
	public void showUpdateDownloadStart() {
		updateProgressDialog = new SimpleProgressDialog<>(this, "Update", null);
		updateProgressDialog.showDialog();
	}

	@Override
	public void showUpdateDownloadEnd() {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
	}
}
