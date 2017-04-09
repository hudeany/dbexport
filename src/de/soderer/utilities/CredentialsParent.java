package de.soderer.utilities;

public interface CredentialsParent {
	public Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword) throws Exception;
}
