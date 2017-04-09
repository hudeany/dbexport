package de.soderer.utilities;

public class Credentials {
	private String username = null;
	private char[] password = null;
	
	public Credentials(String username, char[] password) {
		this.username = username;
		this.password = password;
	}
	
	public Credentials(char[] password) {
		this.password = password;
	}
	
	public Credentials(String username) {
		this.username = username;
	}

	public String getUsername() {
		return username;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public char[] getPassword() {
		return password;
	}
	
	public void setPassword(char[] password) {
		this.password = password;
	}
}
