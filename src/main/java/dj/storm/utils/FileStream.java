package dj.storm.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileStream {
	public final static String outputDirectory = "out/";
	private FileOutputStream stream;
	
	static {
		File outDirectory = new File(outputDirectory);
		if(outDirectory.exists() == false) {
			outDirectory.mkdirs();
		}
	}
	
	public void initalOutput(String name) {
		File file = new File(outputDirectory + name);
		try {
			stream = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void write(String data){
		data += "\n";
		try {
			stream.write(data.getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void close(){
		if (stream == null) {
			return;
		}
		try {
			stream.close();
			stream = null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
