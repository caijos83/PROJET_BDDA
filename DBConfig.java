import java.io.*;
import java.util.Scanner;


public class DBConfig {

	private String dbpath;

	// constructeur qui prend en argument une chaine de caractères correspondant au chemin dbpath
	public DBConfig(String dbpath) {
		this.dbpath = dbpath;
	}

	public static DBConfig LoadDBConfig(String filePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		String line = reader.readLine();
		String dbpath = line.split("=")[1].trim();
		reader.close();
		return new DBConfig(dbpath);
	}

	public String getDbpath() {
		return dbpath;
	}

	public void printConfig() {
		System.out.println("DB Path: " + dbpath);
	}

}