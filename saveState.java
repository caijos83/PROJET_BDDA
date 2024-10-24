public void saveState() throws IOException {
        File dbDir = new File(config.getDbpath()); // Créer une instance File pour le dossier
        if (!dbDir.exists()) {
            dbDir.mkdirs();  // Créer le dossier s'il est inexistant
        }

        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/dm.save"))) {
            out.writeObject(freePages);  // Sérialiser et écrire l'objet freePages dans le fichier
        } // Flux automatiquement fermé ici grace au try-with-ressources
    }
