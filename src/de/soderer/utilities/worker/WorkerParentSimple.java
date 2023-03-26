package de.soderer.utilities.worker;

import java.time.LocalDateTime;

public interface WorkerParentSimple {
	void receiveUnlimitedProgressSignal();

	void receiveProgressSignal(LocalDateTime start, long itemsToDo, long itemsDone);

	void receiveDoneSignal(LocalDateTime start, LocalDateTime end, long itemsDone);

	void changeTitle(String text);

	boolean cancel();
}
