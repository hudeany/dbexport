package de.soderer.utilities.http;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.PasswordGenerator;
import de.soderer.utilities.PasswordGeneratorCharacterGroup;
import de.soderer.utilities.Triple;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;

public class HttpUtilities {
	private static boolean debugLog = false;

	private static TrustManager TRUSTALLCERTS_TRUSTMANAGER = new X509TrustManager() {
		@Override
		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return null;
		}

		@Override
		public void checkClientTrusted(final java.security.cert.X509Certificate[] certificates, final String authType) {
			// nothing to do
		}

		@Override
		public void checkServerTrusted(final java.security.cert.X509Certificate[] certificates, final String authType) {
			// nothing to do
		}
	};

	private static HostnameVerifier TRUSTALLHOSTNAMES_HOSTNAMEVERIFIER = (hostname, session) -> true;

	/**
	 * Use systems default proxy, if set on JVM start.
	 * To override default proxy usage use "executeHttpRequest(httpRequest, Proxy.NO_PROXY)"
	 *
	 * @param httpRequest
	 * @return
	 * @throws Exception
	 */
	public static HttpResponse executeHttpRequest(final HttpRequest httpRequest) throws Exception {
		return executeHttpRequest(httpRequest, null);
	}

	@SuppressWarnings("resource")
	public static HttpResponse executeHttpRequest(final HttpRequest httpRequest, final Proxy proxy) throws Exception {
		try {
			String requestedUrl = httpRequest.getUrlWithProtocol();

			// Check for already in URL included GET parameters
			String parametersFromUrl;
			if (requestedUrl.contains("?")) {
				if (requestedUrl.contains("#")) {
					parametersFromUrl = requestedUrl.substring(requestedUrl.indexOf("?") + 1, requestedUrl.indexOf("#"));
					requestedUrl = requestedUrl.substring(0, requestedUrl.indexOf("?"));
				} else {
					parametersFromUrl = requestedUrl.substring(requestedUrl.indexOf("?") + 1);
					requestedUrl = requestedUrl.substring(0, requestedUrl.indexOf("?"));
				}
			} else {
				parametersFromUrl = "";
			}

			// Prepare GET parameters data
			if (httpRequest.getUrlParameters() != null && httpRequest.getUrlParameters().size() > 0) {
				final String getParameterString = convertToParameterString(httpRequest.getUrlParameters(), httpRequest.getEncoding());
				if (parametersFromUrl.length() > 0) {
					requestedUrl += "?" + parametersFromUrl + "&" + getParameterString;
				} else {
					requestedUrl += "?" + getParameterString;
				}
			} else if (parametersFromUrl.length() > 0) {
				requestedUrl += "?" + parametersFromUrl;
			}

			if (debugLog) {
				System.out.println("Requested URL: " + requestedUrl);
			}

			HttpURLConnection urlConnection;
			if (proxy == null) {
				urlConnection = (HttpURLConnection) new URL(requestedUrl).openConnection(getProxyFromSystem(requestedUrl));
			} else {
				urlConnection = (HttpURLConnection) new URL(requestedUrl).openConnection(proxy);
			}

			if (httpRequest.getRequestMethod() != null) {
				urlConnection.setRequestMethod(httpRequest.getRequestMethod().name());
			}

			if (requestedUrl.startsWith(HttpRequest.SECURE_HTTP_PROTOCOL_SIGN) && !httpRequest.isCheckSslCertificates()) {
				final SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, new TrustManager[] { TRUSTALLCERTS_TRUSTMANAGER }, new java.security.SecureRandom());
				final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
				((HttpsURLConnection) urlConnection).setSSLSocketFactory(sslSocketFactory);
				((HttpsURLConnection) urlConnection).setHostnameVerifier(TRUSTALLHOSTNAMES_HOSTNAMEVERIFIER);
			}

			if (httpRequest.getHeaders() != null && httpRequest.getHeaders().size() > 0) {
				if (debugLog) {
					System.out.println("Request Headers: ");
				}

				for (final Entry<String, String> headerEntry : httpRequest.getHeaders().entrySet()) {
					urlConnection.setRequestProperty(headerEntry.getKey(), headerEntry.getValue());

					if (debugLog) {
						System.out.println(headerEntry.getKey() + ": " + headerEntry.getValue());
					}
				}
			}

			if (httpRequest.getCookieData() != null && httpRequest.getCookieData().size() > 0) {
				final StringBuilder cookieValue = new StringBuilder();
				for (final Entry<String, String> cookieEntry : httpRequest.getCookieData().entrySet()) {
					if (cookieValue.length() > 0) {
						cookieValue.append("; ");
					}
					cookieValue.append(encodeForCookie(cookieEntry.getKey()) + "=" + encodeForCookie(cookieEntry.getValue()));
				}

				urlConnection.setRequestProperty(HttpRequest.HEADER_NAME_UPLOAD_COOKIE, cookieValue.toString());
			}

			final String boundary = HttpUtilities.generateBoundary();

			String httpRequestBody = null;
			if (httpRequest.getRequestBodyContentStream() != null) {
				urlConnection.setDoOutput(true);
				final OutputStream outputStream = urlConnection.getOutputStream();
				IoUtilities.copy(httpRequest.getRequestBodyContentStream(), outputStream);
				outputStream.flush();
			} else if (httpRequest.getRequestBody() != null) {
				urlConnection.setDoOutput(true);
				final OutputStream outputStream = urlConnection.getOutputStream();
				httpRequestBody = httpRequest.getRequestBody();
				outputStream.write(httpRequestBody.getBytes(StandardCharsets.UTF_8));
				outputStream.flush();
			} else if (httpRequest.getUploadFileAttachments() != null && httpRequest.getUploadFileAttachments().size() > 0) {
				urlConnection.setDoOutput(true);
				urlConnection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
				final OutputStream outputStream = urlConnection.getOutputStream();

				if (httpRequest.getPostParameters() != null && httpRequest.getPostParameters().size() > 0) {
					for (final Tuple<String, Object> postParameter : httpRequest.getPostParameters()) {
						outputStream.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
						outputStream.write(("Content-Disposition: form-data; name=\"" + urlEncode(postParameter.getFirst(), StandardCharsets.UTF_8) + "\"\r\n").getBytes(StandardCharsets.UTF_8));
						outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
						if (postParameter.getSecond() != null) {
							outputStream.write(postParameter.getSecond().toString().getBytes(StandardCharsets.UTF_8));
						}
						outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
					}
				}

				for (final Triple<String, String, byte[]> uploadFileAttachment : httpRequest.getUploadFileAttachments()) {
					outputStream.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
					outputStream.write(("Content-Disposition: form-data; name=\"" + uploadFileAttachment.getFirst() + "\"; filename=\"" + uploadFileAttachment.getSecond() + "\"\r\n").getBytes(StandardCharsets.UTF_8));
					outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));

					outputStream.write(uploadFileAttachment.getThird());

					outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
				}

				outputStream.write(("--" + boundary + "--" + "\r\n").getBytes(StandardCharsets.UTF_8));
				outputStream.flush();
			} else if (httpRequest.getPostParameters() != null && httpRequest.getPostParameters().size() > 0) {
				urlConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
				httpRequestBody = convertToParameterString(httpRequest.getPostParameters(), null);

				if (debugLog) {
					System.out.println("Request Body: ");
					System.out.println(httpRequestBody);
				}

				// Send post parameter data
				urlConnection.setDoOutput(true);
				try (OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream(), httpRequest.getEncoding() == null ? StandardCharsets.UTF_8 : httpRequest.getEncoding())) {
					out.write(httpRequestBody);
					out.flush();
				}
			}

			urlConnection.connect();

			final Map<String, String> headers = new LinkedHashMap<>();
			for (final String headerName : urlConnection.getHeaderFields().keySet()) {
				headers.put(headerName, urlConnection.getHeaderField(headerName));
			}

			Charset encoding = StandardCharsets.UTF_8;
			if (headers.containsKey("content-type")) {
				String contentType = headers.get("content-type");
				if (contentType != null && contentType.toLowerCase().contains("charset=")) {
					contentType = contentType.toLowerCase();
					encoding = Charset.forName(contentType.substring(contentType.indexOf("charset=") + 8).trim());
				}
			}

			Map<String, String> cookiesMap = null;
			if (headers.containsKey(HttpRequest.HEADER_NAME_DOWNLOAD_COOKIE)) {
				final String cookiesData = headers.get(HttpRequest.HEADER_NAME_DOWNLOAD_COOKIE);
				if (cookiesData != null) {
					cookiesMap = new LinkedHashMap<>();
					for (final String cookie : cookiesData.split(";")) {
						final String[] cookieParts = cookie.split("=");
						if (cookieParts.length == 2) {
							cookiesMap.put(urlDecode(cookieParts[0].trim(), StandardCharsets.UTF_8), urlDecode(cookieParts[1].trim(), StandardCharsets.UTF_8));
						}
					}
				}
			}

			final int httpResponseCode = urlConnection.getResponseCode();
			if (httpResponseCode < HttpURLConnection.HTTP_BAD_REQUEST) {
				if (httpRequest.getDownloadStream() != null && 200 <= httpResponseCode && httpResponseCode <= 299) {
					IoUtilities.copy(urlConnection.getInputStream(), httpRequest.getDownloadStream());
					return new HttpResponse(httpResponseCode, "File downloaded", urlConnection.getContentType(), headers, cookiesMap);
				} else if (httpRequest.getDownloadFile() != null && 200 <= httpResponseCode && httpResponseCode <= 299) {
					try (FileOutputStream downloadFileOutputStream = new FileOutputStream(httpRequest.getDownloadFile())) {
						IoUtilities.copy(urlConnection.getInputStream(), downloadFileOutputStream);
						return new HttpResponse(httpResponseCode, "File downloaded", urlConnection.getContentType(), headers, cookiesMap);
					} catch (final Exception e) {
						if (httpRequest.getDownloadFile().exists()) {
							httpRequest.getDownloadFile().delete();
						}
						throw e;
					}
				} else {
					try (BufferedReader httpResponseContentReader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), encoding))) {
						final StringBuilder httpResponseContent = new StringBuilder();
						String httpResponseContentLine;
						while ((httpResponseContentLine = httpResponseContentReader.readLine()) != null) {
							if (httpResponseContent.length() > 0) {
								httpResponseContent.append("\n");
							}
							httpResponseContent.append(httpResponseContentLine);
						}
						return new HttpResponse(httpResponseCode, httpResponseContent.toString(), urlConnection.getContentType(), headers, cookiesMap);
					} catch (@SuppressWarnings("unused") final Exception e) {
						return new HttpResponse(httpResponseCode, null, null, headers, cookiesMap);
					}
				}
			} else if ((httpResponseCode == HttpURLConnection.HTTP_MOVED_TEMP || httpResponseCode == HttpURLConnection.HTTP_MOVED_PERM) && httpRequest.isFollowRedirects()) {
				// Optionally follow redirections (HttpCodes 301 and 302)
				final String redirectUrl = urlConnection.getHeaderField("Location");
				if (Utilities.isNotBlank(redirectUrl)) {
					final HttpRequest redirectedHttpRequest = new HttpRequest(httpRequest.getRequestMethod(), redirectUrl);
					return executeHttpRequest(redirectedHttpRequest);
				} else {
					throw new Exception("Redirection url was empty");
				}
			} else {
				try (BufferedReader httpResponseContentReader = new BufferedReader(new InputStreamReader(urlConnection.getErrorStream(), encoding))) {
					final StringBuilder httpResponseContent = new StringBuilder();
					String httpResponseContentLine;
					while ((httpResponseContentLine = httpResponseContentReader.readLine()) != null) {
						if (httpResponseContent.length() > 0) {
							httpResponseContent.append("\n");
						}
						httpResponseContent.append(httpResponseContentLine);
					}
					return new HttpResponse(httpResponseCode, httpResponseContent.toString(), urlConnection.getContentType(), headers, cookiesMap);
				} catch (@SuppressWarnings("unused") final Exception e) {
					return new HttpResponse(httpResponseCode, null, null, headers, cookiesMap);
				}
			}
		} catch (final Exception e) {
			throw e;
		}
	}

	public static String convertToParameterString(final List<Tuple<String, Object>> parameters, Charset encoding) {
		if (parameters == null) {
			return null;
		} else {
			if (encoding == null) {
				encoding = StandardCharsets.UTF_8;
			}
			final StringBuilder returnValue = new StringBuilder();
			for (final Tuple<String, Object> entry : parameters) {
				if (returnValue.length() > 0) {
					returnValue.append("&");
				}
				returnValue.append(urlEncode(entry.getFirst(), encoding));
				returnValue.append("=");
				if (entry.getSecond() != null) {
					returnValue.append(urlEncode(entry.getSecond().toString(), encoding));
				}
			}

			return returnValue.toString();
		}
	}

	public static String urlEncode(final String data, final Charset charset) {
		try {
			return URLEncoder.encode(data, charset.name());
		} catch (final UnsupportedEncodingException e) {
			// Cannot occur, because of the usage of Charset class
			throw new RuntimeException(e);
		}
	}

	public static String urlDecode(final String data, final Charset charset) {
		try {
			return URLDecoder.decode(data, charset.name());
		} catch (final UnsupportedEncodingException e) {
			// Cannot occur, because of the usage of Charset class
			throw new RuntimeException(e);
		}
	}

	/**
	 * This proxy will be used as default proxy.
	 * To override default proxy usage use "Proxy.NO_PROXY"
	 *
	 * It is set via JVM properties on startup:
	 * java ... -Dhttp.proxyHost=proxy.url.local -Dhttp.proxyPort=8080 -Dhttp.nonProxyHosts='127.0.0.1|localhost'
	 */
	public static Proxy getProxyFromSystem(final String url) {
		final String proxyHost = System.getProperty("http.proxyHost");
		if (Utilities.isNotBlank(proxyHost)) {
			final String proxyPort = System.getProperty("http.proxyPort");
			final String nonProxyHosts = System.getProperty("http.nonProxyHosts");

			if (Utilities.isBlank(nonProxyHosts)) {
				if (Utilities.isNotBlank(proxyHost)) {
					if (Utilities.isNotBlank(proxyPort) && NumberUtilities.isNumber(proxyPort)) {
						return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
					} else {
						return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, 8080));
					}
				}
			} else {
				boolean ignoreProxy = false;
				final String urlDomain = getDomainFromUrl(url);
				for (String nonProxyHost : nonProxyHosts.split("\\|")) {
					nonProxyHost = nonProxyHost.trim();
					if (urlDomain == null || urlDomain.equalsIgnoreCase(url)) {
						ignoreProxy = true;
						break;
					}
				}
				if (!ignoreProxy) {
					if (Utilities.isNotBlank(proxyHost)) {
						if (Utilities.isNotBlank(proxyPort) && NumberUtilities.isNumber(proxyPort)) {
							return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
						} else {
							return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, 8080));
						}
					}
				}
			}
		}

		return Proxy.NO_PROXY;
	}

	public static String getDomainFromUrl(String url) {
		if (!url.startsWith("http") && !url.startsWith("https")) {
			url = "http://" + url;
		}
		URL netUrl;
		try {
			netUrl = new URL(url);
		} catch (@SuppressWarnings("unused") final MalformedURLException e) {
			return null;
		}
		return netUrl.getHost();
	}

	public static void pingUrlWithoutSslCheckNoWaitForAnswer(final String pingUrl) throws IOException, NoSuchAlgorithmException, KeyManagementException {
		InputStream downloadStream = null;
		try {
			if (pingUrl.startsWith("https")) {
				// Deactivate SSL-Certificates check
				final SSLContext sslContext = SSLContext.getInstance("TLS");
				final TrustManager[] tms = new TrustManager[] {
						new X509TrustManager() {
							@Override
							public X509Certificate[] getAcceptedIssuers() {
								return null;
							}

							@Override
							public void checkServerTrusted(final X509Certificate[] arg0, final String arg1) throws CertificateException {
								// nothing to do
							}

							@Override
							public void checkClientTrusted(final X509Certificate[] arg0, final String arg1) throws CertificateException {
								// nothing to do
							}
						}
				};
				sslContext.init(null, tms, null);
				final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
				final HttpsURLConnection connection = (HttpsURLConnection) new URL(pingUrl).openConnection();
				connection.setSSLSocketFactory(sslSocketFactory);
				connection.setRequestMethod("POST");
				connection.setConnectTimeout(5000);
				connection.setReadTimeout(100);
				connection.setDoInput(true);
				connection.setDoOutput(false);
				downloadStream = connection.getInputStream();
			} else {
				final HttpURLConnection connection = (HttpURLConnection) new URL(pingUrl).openConnection();
				connection.setRequestMethod("POST");
				connection.setConnectTimeout(5000);
				connection.setReadTimeout(100);
				connection.setDoInput(true);
				connection.setDoOutput(false);
				downloadStream = connection.getInputStream();
			}
		} catch (@SuppressWarnings("unused") final SocketTimeoutException stex) {
			// This Exception is expected for the real short timeout
		} finally {
			Utilities.closeQuietly(downloadStream);
		}
	}

	public static Tuple<String, String> createHtmlFormMimetypeHeader(final Charset encoding) {
		if (encoding == null) {
			return new Tuple<>("content-type", "application/x-www-form-urlencoded");
		} else {
			return new Tuple<>("content-type", "application/x-www-form-urlencoded; charset=" + encoding.name().toLowerCase());
		}
	}

	public static String addUrlParameter(final String url, final String parameterName, final Object parameterValue, final Charset encodingCharSet) {
		final StringBuilder escapedParameterNameAndValue = new StringBuilder();

		if (encodingCharSet == null) {
			escapedParameterNameAndValue.append(parameterName);
		} else {
			escapedParameterNameAndValue.append(urlEncode(parameterName, encodingCharSet));
		}

		escapedParameterNameAndValue.append('=');

		if (parameterValue instanceof char[]) {
			if (encodingCharSet == null) {
				escapedParameterNameAndValue.append(new String((char[]) parameterValue));
			} else {
				escapedParameterNameAndValue.append(urlEncode(new String((char[]) parameterValue), encodingCharSet));
			}
		} else if (parameterValue instanceof Object[]) {
			boolean isFirstValue = true;
			for (final Object value : (Object[]) parameterValue) {
				if (!isFirstValue) {
					escapedParameterNameAndValue.append(",");
				}
				if (encodingCharSet == null) {
					escapedParameterNameAndValue.append(String.valueOf(value));
				} else {
					escapedParameterNameAndValue.append(urlEncode(String.valueOf(value), encodingCharSet));
				}
				isFirstValue = false;
			}
		} else {
			if (encodingCharSet == null) {
				escapedParameterNameAndValue.append(String.valueOf(parameterValue));
			} else {
				escapedParameterNameAndValue.append(urlEncode(String.valueOf(parameterValue), encodingCharSet));
			}
		}
		return addUrlParameter(url, escapedParameterNameAndValue.toString());
	}

	public static String addUrlParameter(final String url, final String escapedParameterNameAndValue) {
		final StringBuilder newUrl = new StringBuilder();
		final int insertPosition = url.indexOf('#');

		if (insertPosition < 0) {
			newUrl.append(url);
			newUrl.append(url.indexOf('?') <= -1 ? '?' : '&');
			newUrl.append(escapedParameterNameAndValue);
		} else {
			newUrl.append(url.substring(0, insertPosition));
			newUrl.append(url.indexOf('?') <= -1 ? '?' : '&');
			newUrl.append(escapedParameterNameAndValue);
			newUrl.append(url.substring(insertPosition));
		}

		return newUrl.toString();
	}

	public static String addPathParameter(final String url, final String escapedParameterNameAndValue) {
		final StringBuilder newUrl = new StringBuilder();
		int insertPosition = url.indexOf('?');
		if (insertPosition < 0) {
			insertPosition = url.indexOf('#');
		}

		if (insertPosition < 0) {
			newUrl.append(url);
			newUrl.append(";");
			newUrl.append(escapedParameterNameAndValue);
		} else {
			newUrl.append(url.substring(0, insertPosition));
			newUrl.append(";");
			newUrl.append(escapedParameterNameAndValue);
			newUrl.append(url.substring(insertPosition));
		}

		return newUrl.toString();
	}

	public static String getPlainParameterFromHtml(final String htmlText, final String parameterName) throws Exception {
		if (Utilities.isBlank(htmlText)) {
			return null;
		} else {
			final Pattern parameterPattern = Pattern.compile("\\W" + parameterName + "\\s*=(\\w*)\\W", Pattern.MULTILINE);
			final Matcher parameterMatcher = parameterPattern.matcher(htmlText);
			if (parameterMatcher.find()) {
				return parameterMatcher.group(1).trim();
			} else {
				return null;
			}
		}
	}

	public static String getQuotedParameterFromHtml(final String htmlText, final String parameterName) throws Exception {
		if (Utilities.isBlank(htmlText)) {
			return null;
		} else {
			final Pattern parameterPattern = Pattern.compile("\\W" + parameterName + "\\s*=\\s\"(\\w*)\"\\W", Pattern.MULTILINE);
			final Matcher parameterMatcher = parameterPattern.matcher(htmlText);
			if (parameterMatcher.find()) {
				return parameterMatcher.group(1).trim();
			} else {
				return null;
			}
		}
	}

	public static String getHttpStatusText(final int httpStatusCode) {
		switch (httpStatusCode) {
			case HttpURLConnection.HTTP_OK:
				// 200
				return "OK";
			case HttpURLConnection.HTTP_CREATED:
				// 201
				return "Created";
			case HttpURLConnection.HTTP_ACCEPTED:
				// 202
				return "Accepted";
			case HttpURLConnection.HTTP_NOT_AUTHORITATIVE:
				// 203
				return "Non-Authoritative Information";
			case HttpURLConnection.HTTP_NO_CONTENT:
				// 204
				return "No Content";
			case HttpURLConnection.HTTP_RESET:
				// 205
				return "Reset Content";
			case HttpURLConnection.HTTP_PARTIAL:
				// 206
				return "Partial Content";
			case HttpURLConnection.HTTP_MULT_CHOICE:
				// 300
				return "Multiple Choices";
			case HttpURLConnection.HTTP_MOVED_PERM:
				// 301
				return "Moved Permanently";
			case HttpURLConnection.HTTP_MOVED_TEMP:
				// 302
				return "Temporary Redirect";
			case HttpURLConnection.HTTP_SEE_OTHER:
				// 303
				return "See Other";
			case HttpURLConnection.HTTP_NOT_MODIFIED:
				// 304
				return "Not Modified";
			case HttpURLConnection.HTTP_USE_PROXY:
				// 305
				return "Use Proxy";
			case HttpURLConnection.HTTP_BAD_REQUEST:
				// 400
				return "Bad Request";
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				// 401
				return "Unauthorized";
			case HttpURLConnection.HTTP_PAYMENT_REQUIRED:
				// 402
				return "Payment Required";
			case HttpURLConnection.HTTP_FORBIDDEN:
				// 403
				return "Forbidden";
			case HttpURLConnection.HTTP_NOT_FOUND:
				// 404
				return "Not Found";
			case HttpURLConnection.HTTP_BAD_METHOD:
				// 405
				return "Method Not Allowed";
			case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
				// 406
				return "Not Acceptable";
			case HttpURLConnection.HTTP_PROXY_AUTH:
				// 407
				return "Proxy Authentication Required";
			case HttpURLConnection.HTTP_CLIENT_TIMEOUT:
				// 408
				return "Request Time-Out";
			case HttpURLConnection.HTTP_CONFLICT:
				// 409
				return "Conflict";
			case HttpURLConnection.HTTP_GONE:
				// 410
				return "Gone";
			case HttpURLConnection.HTTP_LENGTH_REQUIRED:
				// 411
				return "Length Required";
			case HttpURLConnection.HTTP_PRECON_FAILED:
				// 412
				return "Precondition Failed";
			case HttpURLConnection.HTTP_ENTITY_TOO_LARGE:
				// 413
				return "Request Entity Too Large";
			case HttpURLConnection.HTTP_REQ_TOO_LONG:
				// 414
				return "Request-URI Too Large";
			case HttpURLConnection.HTTP_UNSUPPORTED_TYPE:
				// 415
				return "Unsupported Media Type";
			case HttpURLConnection.HTTP_INTERNAL_ERROR:
				// 500
				return "Internal Server Error";
			case HttpURLConnection.HTTP_NOT_IMPLEMENTED:
				// 501
				return "Not Implemented";
			case HttpURLConnection.HTTP_BAD_GATEWAY:
				// 502
				return "Bad Gateway";
			case HttpURLConnection.HTTP_UNAVAILABLE:
				// 503
				return "Service Unavailable";
			case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
				// 504
				return "Gateway Timeout";
			case HttpURLConnection.HTTP_VERSION:
				// 505
				return "HTTP Version Not Supported";
			default:
				return "Unknown Http Status Code (" + httpStatusCode + ")";
		}
	}

	public static String createBasicAuthenticationHeaderValue(final String username, final String password) {
		return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
	}

	private static String encodeForCookie(final String value) {
		if (value == null) {
			return value;
		} else {
			return value.replace(";", "%3B").replace("=", "%3D");
		}
	}

	public static String generateBoundary() throws Exception {
		final List<PasswordGeneratorCharacterGroup> groups = new ArrayList<>();
		groups.add(new PasswordGeneratorCharacterGroup("abcdefghijklmnopqrstuvwxyz".toCharArray()));
		groups.add(new PasswordGeneratorCharacterGroup("ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray()));
		groups.add(new PasswordGeneratorCharacterGroup("0123456789".toCharArray()));
		return new String(PasswordGenerator.generatePassword(groups, 32, 32));
	}

	public static X509Certificate getServerTlsCertificate(final String hostnameOrIp, final int port) throws Exception {
		final HttpsURLConnection urlConnection = (HttpsURLConnection) new URL("https://" + hostnameOrIp + ":" + port).openConnection();
		final SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, new TrustManager[] { TRUSTALLCERTS_TRUSTMANAGER }, new java.security.SecureRandom());
		final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
		urlConnection.setSSLSocketFactory(sslSocketFactory);
		urlConnection.setHostnameVerifier(TRUSTALLHOSTNAMES_HOSTNAMEVERIFIER);
		urlConnection.connect();
		final Certificate[] certificates = urlConnection.getServerCertificates();
		for (final Certificate certificate : certificates) {
			if (certificate instanceof X509Certificate) {
				// Take the first certificate with alternative names
				if (((X509Certificate)certificate).getSubjectAlternativeNames() != null) {
					return (X509Certificate) certificate;
				}
			}
		}
		for (final Certificate certificate : certificates) {
			if (certificate instanceof X509Certificate) {
				// Take the first X509Certificate available, even without alternative names
				return (X509Certificate) certificate;
			}
		}
		return null;
	}

	public static void createTrustStoreFile(final String hostnameOrIpAndPort, final int defaultPort, final File trustStoreFile, final char[] trustStorePassword) throws Exception {
		if (trustStoreFile.exists()) {
			throw new Exception("File '" + trustStoreFile.getAbsolutePath() + "' already exists");
		}

		String hostnameOrIp;
		int port;
		final String[] hostParts = hostnameOrIpAndPort.split(":");
		if (hostParts.length == 2) {
			hostnameOrIp = hostParts[0];
			try {
				port = Integer.parseInt(hostParts[1]);
			} catch (@SuppressWarnings("unused") final Exception e) {
				throw new Exception("Invalid port: " + hostParts[1]);
			}
		} else {
			hostnameOrIp = hostnameOrIpAndPort;
			port = defaultPort;
		}

		final X509Certificate certificate = getServerTlsCertificate(hostnameOrIp, port);
		if (certificate == null) {
			throw new Exception("Cannot get TLS certificate for '" + hostnameOrIp + ":" + port + "'");
		}

		final KeyStore keyStore = KeyStore.getInstance("JKS");
		keyStore.load(null);

		final char[] password = trustStorePassword == null ? new char[0] : trustStorePassword;
		keyStore.setCertificateEntry(hostnameOrIp, certificate);

		try (OutputStream javaKeyStoreOutputStream = new FileOutputStream(trustStoreFile)) {
			keyStore.store(javaKeyStoreOutputStream, password);
		}
	}
}
