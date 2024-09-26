package projet_SGBD;
import java.io.*;
import java.util.Scanner;
import java.lang.Math;
public class DBConfig {
	String dbpath;
	int pagesize;
	long dm_maxfilesize;
	
	DBConfig(String dbpath){
		this.dbpath=dbpath;
	}
	DBConfig(String dbpath,int pagesize,long dm_maxfilesize){
		this.dbpath=dbpath;
		this.pagesize=pagesize;
		this.dm_maxfilesize=dm_maxfilesize;
	}
	public String getDbath() {
		return dbpath;
	}
	public int getPageSize() {
		return pagesize;
	}
	public long getDm_maxfilesize() {
		return dm_maxfilesize;
	}

	public static DBConfig LoadDBConfig(File fichier_config) throws FileNotFoundException{
		DBConfig dbConfig=null;
		Scanner myReader = new Scanner(fichier_config);
		int pagesize=0;
		int ligne=0;
		while(myReader.hasNextLine()) {
			String data = myReader.nextLine();
			int pagetotal=(int) Math.ceil((double)fichier_config.length()/4096);
			System.out.println("page total"+pagetotal);
			ligne+=data.length();
			System.out.println("ligne"+ligne);
			
			pagesize=(int)Math.ceil((double)ligne/4096);
			System.out.println("*******page"+pagesize);
			
			dbConfig=new DBConfig(data,pagesize,fichier_config.length());
			
			dbConfig.afficheinfo();
			
		}
		myReader.close();
		return dbConfig;
		}
	
	public void afficheinfo() {
		System.out.println("contenu:"+dbpath+"\n"+"page:"+pagesize+"\n"+"taille:"+dm_maxfilesize);
	}
}
