package de.soderer.utilities;

import java.util.Date;

public interface UpdateParent extends CredentialsParent {
	public void showUpdateError(String errorText);

	public void showUpdateProgress(Date start, long itemsToDo, long itemsDone);

	public void showUpdateDone();

	public boolean askForUpdate(String availableNewVersion) throws Exception;

	public void showUpdateDownloadStart();
	
	public void showUpdateDownloadEnd();
}
