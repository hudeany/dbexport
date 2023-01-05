package de.soderer.utilities.worker;

import java.time.LocalDateTime;

public interface WorkerParentSimple {
	void showUnlimitedProgress();

	void showProgress(LocalDateTime start, long itemsToDo, long itemsDone);

	void showDone(LocalDateTime start, LocalDateTime end, long itemsDone);

	void changeTitle(String text);

	boolean cancel();
}
