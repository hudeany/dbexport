package de.soderer.utilities;

import java.util.Date;

public abstract class WorkerDual<T> extends WorkerSimple<T> {
	protected Date startTimeSub = null;
	protected Date endTimeSub = null;
	protected String currentItemName = null;
	protected long subItemsToDo = -1;
	protected long subItemsDone = -1;

	public WorkerDual(WorkerParentDual parent) {
		super(parent);
	}

	protected void showItemStart(String itemName) {
		currentItemName = itemName;
		startTimeSub = new Date();
		if (parent != null && !cancel) {
			((WorkerParentDual) parent).showItemStart(currentItemName);
		}
	}

	protected void showItemProgress() {
		showItemProgress(false);
	}

	protected void showItemProgress(boolean overrideRefreshTime) {
		if (parent != null && !cancel) {
			if (new Date().getTime() - lastProgressShow.getTime() > showProgressAfterMilliseconds) {
				// Normal progress update
				((WorkerParentDual) parent).showItemProgress(startTimeSub, subItemsToDo, subItemsDone);
				lastProgressShow = new Date();
			} else if (overrideRefreshTime) {
				// Important progress update, which may not be left out
				((WorkerParentDual) parent).showItemProgress(startTimeSub, subItemsToDo, subItemsDone);
				lastProgressShow = new Date();
			}
		}
	}

	protected void showItemDone() {
		endTimeSub = new Date();
		if (parent != null) {
			((WorkerParentDual) parent).showItemDone(startTimeSub, endTimeSub, subItemsDone);
			currentItemName = null;
		}
	}

	protected void showUnlimitedSubProgress() {
		if (parent != null) {
			((WorkerParentDual) parent).showUnlimitedSubProgress();
		}
	}
}
