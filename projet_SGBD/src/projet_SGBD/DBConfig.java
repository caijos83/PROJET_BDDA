package projet_SGBD;
import java.io.*;



public class DBConfig {

	private String dbpath;
	private int pagesize;
	private long dm_maxfilesize;


	public DBConfig(String dbpath, int pagesize, long dm_maxfilesize) {
		this.dbpath = dbpath;
		this.pagesize = pagesize;
		this.dm_maxfilesize = dm_maxfilesize;
	}


	public static DBConfig LoadDBConfig(String filePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		String line;
		String dbpath = null;
		int pagesize = 0;
		long dm_maxfilesize = 0;

		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("=");
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
			}
		}
		reader.close();
		return new DBConfig(dbpath, pagesize, dm_maxfilesize);
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

	public void printConfig() {
		System.out.println("DB Path: " + dbpath);
		System.out.println("Page Size: " + pagesize + " bytes");
		System.out.println("Max File Size: " + dm_maxfilesize + " bytes");
	}



}
