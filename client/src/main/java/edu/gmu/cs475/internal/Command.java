package edu.gmu.cs475.internal;

import edu.gmu.cs475.AbstractKeyValueClient;
import edu.gmu.cs475.KeyValueClient;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.rmi.RemoteException;
import java.util.Map;

@ShellComponent
public class Command {

	static AbstractKeyValueClient service;

	public Command() {
		service = new KeyValueClient("127.0.0.1", Main.port);
	}


	@ShellMethod("List a directory")
	public CharSequence listDirectory(String directory) {
		try {
			StringBuilder ret = new StringBuilder();
			for (String s : service.listDirectory(directory)) {
				ret.append(' ' + s);
			}
			return ret.toString();
		} catch (Throwable e) {
			e.printStackTrace();
			return new AttributedString("Unexpected exception", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}

	@ShellMethod("List all keys")
	public CharSequence listKeys() {
		StringBuilder ret = new StringBuilder();
		try {
			for (String t : service.listKeys()) {
				ret.append(' ' + t);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			return new AttributedString("Unexpected exception", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
		return ret.toString();
	}

	@ShellMethod("Get a key's value")
	public CharSequence get(String key) {
		try {
			return service.get(key);
		} catch (Throwable e) {
			e.printStackTrace();
			return new AttributedString("Unexpected exception", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}

	@ShellMethod("Set a key's value")
	public CharSequence set(String key, String value) {
		try {
			service.set(key, value);
			return null;
		} catch (Throwable e) {
			e.printStackTrace();
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}

	@ShellMethod("Update all keys in a directory to have the given content")
	public CharSequence putAll(String directory, String content) {
		try {
			service.putAll(directory, content);
			return null;
		} catch (Throwable e) {
			e.printStackTrace();
			return new AttributedString("Error: " + e.getMessage(), AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
		}
	}
}

