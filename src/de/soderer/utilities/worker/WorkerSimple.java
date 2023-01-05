package de.soderer.utilities.worker;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class WorkerSimple<T> implements RunnableFuture<T> {
	private static final long DEFAULT_SHOW_PROGRESS_AFTER_MILLISECONDS = 500;

	private LocalDateTime startTime;
	private LocalDateTime endTime;
	protected long itemsToDo = -1;
	protected long itemsDone = -1;
	protected LocalDateTime lastProgressShow = LocalDateTime.now();
	protected long showProgressAfterMilliseconds = DEFAULT_SHOW_PROGRESS_AFTER_MILLISECONDS;
	private T result = null;

	protected WorkerParentSimple parent;

	private Exception error = null;
	protected boolean cancel = false;
	private boolean isDone = false;

	public WorkerSimple(final WorkerParentSimple parent) {
		this.parent = parent;
	}

	public void setParent(final WorkerParentSimple parent) {
		this.parent = parent;
	}

	/**
	 * Minimum time between two parent.showProgress calls. (default 500 millis)
	 *
	 * @param value
	 */
	public void setShowProgressAfterMilliseconds(final long value) {
		showProgressAfterMilliseconds = value;
	}

	public void showProgress() {
		showProgress(false);
	}

	public void showProgress(final boolean overrideRefreshTime) {
		if (parent != null && !cancel) {
			if (Duration.between(lastProgressShow, LocalDateTime.now()).toMillis() > showProgressAfterMilliseconds) {
				// Normal progress update
				parent.showProgress(startTime, itemsToDo, itemsDone);
				lastProgressShow = LocalDateTime.now();
			} else if (overrideRefreshTime) {
				// Important progress update, which may not be left out
				parent.showProgress(startTime, itemsToDo, itemsDone);
				lastProgressShow = LocalDateTime.now();
			}
		}
	}

	public void showUnlimitedProgress() {
		if (parent != null) {
			parent.showUnlimitedProgress();
		}
	}

	public boolean cancel() {
		return cancel(false);
	}

	@Override
	public boolean cancel(final boolean waitForDone) {
		if (!cancel) {
			cancel = true;
			if (parent != null) {
				parent.cancel();
			}
		}
		if (waitForDone) {
			while (!isDone()) {
				try {
					Thread.sleep(500);
				} catch (@SuppressWarnings("unused") final InterruptedException e) {
					// do nothing
				}
			}
		}
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
	public T get(final long arg0, final TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
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

	public LocalDateTime getStartTime() {
		return startTime;
	}

	public void setEndTime(final LocalDateTime endTime) {
		this.endTime = endTime;
	}

	public LocalDateTime getEndTime() {
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
		boolean doIt;
		synchronized (this) {
			if (startTime != null) {
				doIt = false;
			} else {
				startTime = LocalDateTime.now();
				doIt = true;
			}
		}

		if (doIt) {
			result = null;
			error = null;
			cancel = false;
			isDone = false;
			itemsToDo = 0L;
			itemsDone = 0L;

			try {
				result = work();
			} catch (final Exception e) {
				error = e;
			} catch (final Throwable e) {
				error = new Exception("Fatal error occurred: " + e.getMessage(), e);
			}

			if (endTime == null) {
				endTime = LocalDateTime.now();
			}
			isDone = true;
			if (parent != null) {
				parent.showDone(startTime, endTime, itemsDone);
			}
		}
	}

	public abstract T work() throws Exception;
}
