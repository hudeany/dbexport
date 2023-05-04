package de.soderer.utilities.worker;

import java.time.LocalDateTime;

public interface WorkerParentDual extends WorkerParentSimple {
	void receiveUnlimitedSubProgressSignal();

	void receiveItemStartSignal(String itemName, String description);

	void receiveItemProgressSignal(LocalDateTime itemStart, long subItemToDo, long subItemDone, String itemsUnitSign);

	void receiveItemDoneSignal(LocalDateTime itemStart, LocalDateTime itemEnd, long subItemsDone, String itemsUnitSign, String resultText);
}
