public void loadState() throws IOException, ClassNotFoundException {
        // On commence par vérifier si le fichier que l'on veut restaurer est au préalable existant
        File saveFile = new File(config.getDbpath() + "/dm.save");

        if (!saveFile.exists()) {
            System.out.println("Aucun état sauvegardé trouvé.");
            return;  // Si le fichier n'existe pas, sortir de la méthode
        }
        // S'il existe bien, on restaure ses pages libres :
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(config.getDbpath() + "/dm.save"))){
            freePages = (List<PageId>) in.readObject();
        } // Flux automatiquement fermé ici grace au try-with-ressources
    }
