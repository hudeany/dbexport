package de.soderer.utilities;

import java.util.Date;

public interface WorkerParentSimple {
	public void showUnlimitedProgress();

	public void showProgress(Date start, long itemsToDo, long itemsDone);

	public void showDone(Date start, Date end, long itemsDone);
	
	public void changeTitle(String text);

	public void cancel();
}
