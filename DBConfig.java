import java.io.*;
import java.util.Scanner;


public class DBConfig {

	private String dbpath;
	private int pagesize;
	private long dm_maxfilesize;
	private int bm_buffercount;
	private String bm_policy; // Politique de remplacement (LRU/MRU)


	public DBConfig(String dbpath, int pagesize, long dm_maxfilesize, int bm_buffercount, String bm_policy) {
		this.dbpath = dbpath;
		this.pagesize = pagesize;
		this.dm_maxfilesize = dm_maxfilesize;
		this.bm_buffercount = bm_buffercount;
		this.bm_policy = bm_policy;
	}


	public static DBConfig LoadDBConfig(String filePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		String line;
		String dbpath = null;
		int pagesize = 0;
		long dm_maxfilesize = 0;
		int bm_buffercount = 0;
		String bm_policy = null;

		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("=");
			if (parts.length == 2) {
				switch (parts[0].trim()) {
					case "dbpath":
						dbpath = parts[1].trim();
						break;
					case "pagesize":
						pagesize = Integer.parseInt(parts[1].trim());
						break;
					case "dm_maxfilesize":
						dm_maxfilesize = Long.parseLong(parts[1].trim());
						break;
					case "bm_buffercount":
						bm_buffercount = Integer.parseInt(parts[1].trim());
						break;
					case "bm_policy":
						bm_policy = parts[1].trim();
						break;
				}
			}
		}
		reader.close();
		return new DBConfig(dbpath, pagesize, dm_maxfilesize, bm_buffercount, bm_policy);
	}


	public String getDbpath() {
		return dbpath;
	}


	public int getPagesize() {
		return pagesize;
	}


	public long getDm_maxfilesize() {
		return dm_maxfilesize;
	}

	public int getBm_buffercount(){
		return bm_buffercount;
	}

	public String getBm_policy() {
		return bm_policy;
	}

	public void printConfig() {
		System.out.println("DB Path: " + dbpath);
		System.out.println("Page Size: " + pagesize + " bytes");
		System.out.println("Max File Size: " + dm_maxfilesize + " bytes");
		System.out.println("Buffer Count: " + bm_buffercount);
		System.out.println("Replacement: " + bm_policy);
	}


	public int getBmBuffercount() {
		return bm_buffercount;
	}

	public String getBmPolicy() {
		return bm_policy;
	}


}