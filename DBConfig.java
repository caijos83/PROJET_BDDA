
package projet_SGBD;
import java.io.*;
//test 
public class DBConfig {
	public String dbpath;
	DBConfig(String dbpath){
		this.dbpath=dbpath;
	}
	public String getDbath() {
		return dbpath;
	}
public static DBConfig LoadDBConfig(String fichier_config) throws IOException{
	StringBuffer sb = new StringBuffer();
		return fichier_config;
	}
}
