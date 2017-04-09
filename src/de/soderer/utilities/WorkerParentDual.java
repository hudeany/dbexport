package de.soderer.utilities;

import java.util.Date;

public interface WorkerParentDual extends WorkerParentSimple {
	public void showUnlimitedSubProgress();

	public void showItemStart(String itemName);

	public void showItemProgress(Date itemStart, long subItemToDo, long subItemDone);

	public void showItemDone(Date itemStart, Date itemEnd, long subItemsDone);
}
