
package projet_SGBD;
import java.io.*;
import java.util.Scanner;
public class DBConfig {
	String dbpath;
	
	DBConfig(String dbpath){
		this.dbpath=dbpath;
	}
	public String getDbath() {
		return dbpath;
	}
public static DBConfig LoadDBConfig(File fichier_config) throws FileNotFoundException{
	DbConfg dbConfig=null;
	Scanner myReader = new Scanner(fichier_config);
	if (myReader.hasNextLine()) {
		String data = myReader.nextLine();
		dbConfig=new DBConfig(data);
	}
	myReader.close();
	return dbConfig;
	}
}
