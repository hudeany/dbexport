package de.soderer.utilities;

import java.io.File;
import java.util.Date;

public class FileChangeWatcherThread extends Thread {
	protected File file;
	protected NotificationListener listener;
	protected Date lastChangeDate = null;
	protected long lastFileSize = 0L;
	private static final long delay = 1000L;
	protected boolean running = true;

	public FileChangeWatcherThread(File file, NotificationListener listener) {
		this.file = file;
		this.listener = listener;
		setDaemon(true);
	}

	@Override
	public void run() {
		try {
			while (running) {
				if (!file.exists()) {
					running = false;
					listener.noticeFileNotExists();
				} else {
					if (lastChangeDate == null || lastChangeDate.before(new Date(file.lastModified())) || lastFileSize != file.length()) {
						lastChangeDate = new Date(file.lastModified());
						lastFileSize = file.length();
						listener.noticeFileChanged();
					}

					Thread.sleep(delay);
				}
			}
		} catch (InterruptedException IE) {
			handleInterrupt();
		}
	}

	protected void handleInterrupt() {
		if (running) {
			run();
		}
	}

	public void kill() {
		running = false;
		interrupt();
	}

	public interface NotificationListener {
		public void noticeFileChanged();

		public void noticeFileNotExists();
	}
}
