package projet_SGBD;
import java.io.*;

import java.lang.Math;

public class TestDBConfig {

	public static void main(String[] args) throws FileNotFoundException {

		System.out.println("db n°1 avec un constructeur ayant un seul argument(dbpath):");
		DBConfig db=new DBConfig("/users/licence/io04873/Bureau/PROJET_BDDA");
		db.afficheinfo();
		
		System.out.println("db n°2 avec un constructeur ayant 3 argument(dbpath):");
		File fichier=new File("/users/licence/io04873/Bureau/PROJET_BDDA/tete.txt");
		DBConfig db2 = DBConfig.LoadDBConfig(fichier);
		
		db2.afficheinfo();
		// calcule de la page
		double nb=(double)4748/4096;
		System.out.println(nb);
		System.out.println(Math.ceil(nb));
	}

}
