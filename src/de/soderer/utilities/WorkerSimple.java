package de.soderer.utilities;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class WorkerSimple<T> implements RunnableFuture<T> {
	private static final long DEFAULT_SHOW_PROGRESS_AFTER_MILLISECONDS = 500;

	private Date startTime;
	private Date endTime;
	protected long itemsToDo = -1;
	protected long itemsDone = -1;
	protected long dataItemsDone = -1;
	protected Date lastProgressShow = new Date();
	protected long showProgressAfterMilliseconds = DEFAULT_SHOW_PROGRESS_AFTER_MILLISECONDS;
	private T result = null;

	protected WorkerParentSimple parent;

	private Exception error = null;
	protected boolean cancel = false;
	private boolean isDone = false;

	public WorkerSimple(WorkerParentSimple parent) {
		this.parent = parent;
	}
	
	public void setParent(WorkerParentSimple parent) {
		this.parent = parent;
	}

	/**
	 * Minimum time between two parent.showProgress calls. (default 500 millis)
	 *
	 * @param value
	 */
	public void setShowProgressAfterMilliseconds(long value) {
		showProgressAfterMilliseconds = value;
	}

	protected void showProgress() {
		showProgress(false);
	}

	protected void showProgress(boolean overrideRefreshTime) {
		if (parent != null && !cancel) {
			if (new Date().getTime() - lastProgressShow.getTime() > showProgressAfterMilliseconds) {
				// Normal progress update
				parent.showProgress(startTime, itemsToDo, itemsDone);
				lastProgressShow = new Date();
			} else if (overrideRefreshTime) {
				// Important progress update, which may not be left out
				parent.showProgress(startTime, itemsToDo, itemsDone);
				lastProgressShow = new Date();
			}
		}
	}

	protected void showUnlimitedProgress() {
		if (parent != null) {
			parent.showUnlimitedProgress();
		}
	}

	@Override
	public boolean cancel(boolean arg0) {
		return cancel();
	}

	public boolean cancel() {
		cancel = true;
		return cancel;
	}

	/**
	 * Check for error and return result value
	 */
	@Override
	public T get() throws ExecutionException {
		if (!isDone) {
			throw new ExecutionException(new Exception("Worker is not done yet"));
		} else if (error != null) {
			throw new ExecutionException(error);
		} else {
			return result;
		}
	}
	
	public Exception getError() {
		return error;
	}

	@Override
	public T get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
		return get();
	}

	@Override
	public boolean isCancelled() {
		return cancel;
	}

	@Override
	public boolean isDone() {
		return isDone;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public long getItemsToDo() {
		return itemsToDo;
	}

	public long getItemsDone() {
		return itemsDone;
	}

	@Override
	public final void run() {
		result = null;
		error = null;
		cancel = false;
		isDone = false;
		itemsToDo = 0L;
		itemsDone = 0L;
		startTime = new Date();
		
		try {
			result = work();
		} catch (Exception e) {
			error = e;
		} catch (Throwable e) {
			error = new Exception("Fatal error occurred: " + e.getMessage(), e);
		}
		
		if (endTime == null) {
			endTime = new Date();
		}
		isDone = true;
		if (parent != null) {
			parent.showDone(startTime, endTime, itemsDone);
		}
	}
	
	public abstract T work() throws Exception;
}
