package de.soderer.utilities.console;

public class Size {
	int height;
	int width;

	public Size(final int height, final int width) {
		super();
		this.height = height;
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public int getWidth() {
		return width;
	}

	@Override
	public String toString() {
		return "Height: " + height + ", Width: " + width;
	}
}
