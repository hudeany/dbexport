package de.soderer.utilities.worker;

import java.time.LocalDateTime;

public interface WorkerParentDual extends WorkerParentSimple {
	void showUnlimitedSubProgress();

	void showItemStart(String itemName);

	void showItemProgress(LocalDateTime itemStart, long subItemToDo, long subItemDone);

	void showItemDone(LocalDateTime itemStart, LocalDateTime itemEnd, long subItemsDone);
}
