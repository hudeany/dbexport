package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class FileDownloadWorker extends WorkerSimple<Boolean> {
	private String downloadUrl;
	private File destinationFile;

	public FileDownloadWorker(WorkerParentSimple parent, String downloadUrl, File destinationFile) {
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

			BufferedInputStream bufferedInputStream = null;
			FileOutputStream fileOutputStream = null;
			try {
				showUnlimitedProgress();
				URLConnection urlConnection = new URL(downloadUrl).openConnection();
				itemsToDo = urlConnection.getContentLength();
				InputStream inputStream = urlConnection.getInputStream();
				showProgress(true);
				bufferedInputStream = new BufferedInputStream(inputStream);
				fileOutputStream = new FileOutputStream(destinationFile);

				byte[] buffer = new byte[4096];
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
			} catch (Exception e) {
				if (e.getMessage().toLowerCase().contains("server returned http response code: 401")) {
					throw new UserError("error.userNotAuthenticatedOrNotAuthorized", UserError.Reason.UnauthenticatedOrUnauthorized);
				} else {
					throw new Exception("Cannot download file: " + e.getMessage(), e);
				}
			} finally {
				Utilities.closeQuietly(fileOutputStream);
				Utilities.closeQuietly(bufferedInputStream);
			}

			if (cancel) {
				destinationFile.delete();
				return false;
			} else {
				showProgress(true);
				return true;
			}
		} catch (Exception e) {
			throw e;
		}
	}
}
