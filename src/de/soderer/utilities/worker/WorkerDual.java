package de.soderer.utilities.worker;

import java.time.Duration;
import java.time.LocalDateTime;

public abstract class WorkerDual<T> extends WorkerSimple<T> {
	protected LocalDateTime startTimeSub = null;
	protected LocalDateTime endTimeSub = null;
	protected String currentItemName = null;
	protected long subItemsToDo = -1;
	protected long subItemsDone = -1;

	public WorkerDual(final WorkerParentDual parent) {
		super(parent);
	}

	protected void showItemStart(final String itemName) {
		currentItemName = itemName;
		startTimeSub = LocalDateTime.now();
		if (parent != null && !cancel) {
			((WorkerParentDual) parent).showItemStart(currentItemName);
		}
	}

	protected void showItemProgress() {
		showItemProgress(false);
	}

	protected void showItemProgress(final boolean overrideRefreshTime) {
		if (parent != null && !cancel) {
			if (Duration.between(lastProgressShow, LocalDateTime.now()).toMillis() > showProgressAfterMilliseconds) {
				// Normal progress update
				((WorkerParentDual) parent).showProgress(startTimeSub, itemsToDo, itemsDone);
				((WorkerParentDual) parent).showItemProgress(startTimeSub, subItemsToDo, subItemsDone);
				lastProgressShow = LocalDateTime.now();
			} else if (overrideRefreshTime) {
				// Important progress update, which may not be left out
				((WorkerParentDual) parent).showProgress(startTimeSub, itemsToDo, itemsDone);
				((WorkerParentDual) parent).showItemProgress(startTimeSub, subItemsToDo, subItemsDone);
				lastProgressShow = LocalDateTime.now();
			}
		}
	}

	protected void showItemDone() {
		endTimeSub = LocalDateTime.now();
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
