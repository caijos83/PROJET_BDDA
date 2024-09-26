package projet_SGBD;

import java.io.*;

public class TestDBConfig {

	public static void main(String[] args) throws FileNotFoundException {
		System.out.println("db n°1 avec un constructeur ayant un seul argument(dbpath):");
		DBConfig db=new DBConfig("/users/licence/io04873/Bureau/PROJET_BDDA");
		System.out.println("db n°2 avec un constructeur ayant 3 argument(dbpath):");
		File fichier=new File("/users/licence/io04873/Bureau/PROJET_BDDA/tete.txt");
		DBConfig db2 = DBConfig.LoadDBConfig(fichier);
		db.afficheinfo();
		db2.afficheinfo();

	}

}
