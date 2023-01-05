package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;

import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;

public class FileDownloadWorker extends WorkerSimple<Boolean> {
	private final String downloadUrl;
	private final File destinationFile;

	public FileDownloadWorker(final WorkerParentSimple parent, final String downloadUrl, final File destinationFile) {
		super(parent);

		this.destinationFile = destinationFile;
		this.downloadUrl = downloadUrl;
	}

	@Override
	public Boolean work() throws Exception {
		showProgress();

		try {
			if (destinationFile.exists()) {
				throw new Exception("File already exists: " + destinationFile.getAbsolutePath());
			}

			showUnlimitedProgress();
			final URLConnection urlConnection = new URL(downloadUrl).openConnection();
			itemsToDo = urlConnection.getContentLength();
			showProgress(true);
			try (BufferedInputStream bufferedInputStream = new BufferedInputStream(urlConnection.getInputStream());
					FileOutputStream fileOutputStream = new FileOutputStream(destinationFile)) {
				final byte[] buffer = new byte[4096];
				int readLength;
				while ((readLength = bufferedInputStream.read(buffer)) != -1) {
					if (cancel) {
						break;
					} else {
						fileOutputStream.write(buffer, 0, readLength);
						itemsDone += readLength;
						showProgress();
					}
				}
				Utilities.clear(buffer);
			} catch (final Exception e) {
				if (e.getMessage().toLowerCase().contains("server returned http response code: 401")) {
					throw new UserError("error.userNotAuthenticatedOrNotAuthorized", UserError.Reason.UnauthenticatedOrUnauthorized);
				} else {
					throw new Exception("Cannot download file: " + e.getMessage(), e);
				}
			}

			if (cancel) {
				destinationFile.delete();
				return false;
			} else {
				showProgress(true);
				return true;
			}
		} catch (final Exception e) {
			throw e;
		}
	}
}
