package de.soderer.utilities;

public interface CredentialsParent {
	Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword, boolean firstRequest) throws Exception;
}
