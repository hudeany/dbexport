package de.soderer.utilities.worker;

import java.time.LocalDateTime;

public interface WorkerParentSimple {
	void receiveUnlimitedProgressSignal();

	void receiveProgressSignal(LocalDateTime start, long itemsToDo, long itemsDone, String itemsUnitSign);

	void receiveDoneSignal(LocalDateTime start, LocalDateTime end, long itemsDone, String itemsUnitSign, String resultText);

	void changeTitle(String text);

	boolean cancel();
}
