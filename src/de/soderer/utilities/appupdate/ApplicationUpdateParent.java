package de.soderer.utilities.appupdate;

import java.time.LocalDateTime;

import de.soderer.utilities.CredentialsParent;
import de.soderer.utilities.Version;
import de.soderer.utilities.worker.WorkerSimple;

public interface ApplicationUpdateParent extends CredentialsParent {
	void showUpdateError(String errorText);

	void showUpdateProgress(LocalDateTime start, long itemsToDo, long itemsDone, String itemsUnitSign);

	void showUpdateDone(LocalDateTime startTime, LocalDateTime endTime, long itemsDone, String itemsUnitSign);

	boolean askForUpdate(Version availableNewVersion) throws Exception;

	void showUpdateDownloadStart(WorkerSimple<Boolean> worker);

	void showUpdateDownloadEnd(LocalDateTime startTime, LocalDateTime endTime, long itemsDone, String itemsUnitSign);
}
