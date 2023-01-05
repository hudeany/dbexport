package de.soderer.utilities.http;

import java.util.Map;
import java.util.Map.Entry;

public class HttpResponse {
	private final int httpCode;
	private final String content;
	private final String contentType;
	private final Map<String, String> headers;
	private final Map<String, String> cookieData;

	public HttpResponse(final int httpCode, final String content, final String contentType, final Map<String, String> headers, final Map<String, String> cookieData) {
		this.httpCode = httpCode;
		this.content = content;
		this.contentType = contentType;
		this.headers = headers;
		this.cookieData = cookieData;
	}

	public int getHttpCode() {
		return httpCode;
	}

	public String getContent() {
		return content;
	}

	public String getContentType() {
		return contentType;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public Map<String, String> getCookies() {
		return cookieData;
	}

	@Override
	public String toString() {
		String returnText = "HttpCode: " + httpCode + "\n";
		if (headers != null && headers.size() > 0) {
			returnText += "HttpHeaders:\n";
			for (final Entry<String, String> entry : headers.entrySet()) {
				returnText += "\t" + entry.getKey() + ": " + entry.getValue() + "\n";
			}
		}
		if (cookieData != null && cookieData.size() > 0) {
			returnText += "HttpCookies:\n";
			for (final Entry<String, String> entry : cookieData.entrySet()) {
				returnText += "\t" + entry.getKey() + ": " + entry.getValue() + "\n";
			}
		}
		returnText += "ContentType: " + contentType + "\n";
		returnText += "Content: \n" + content + "\n";
		return returnText;
	}
}
