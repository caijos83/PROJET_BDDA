public PageId allocPage() throws IOException {
        // Si on a des pages libres, les utiliser en priorité
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        // Parcourir les fichiers existants pour trouver de l’espace
        File dbDir = new File(config.getDbpath());
        // crée un tableau de page contenant les fichier se terminant par l'extension rsdb
        File[] fichiers = dbDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        if (fichiers != null) {
            for (File fichier : fichiers) {
                // Si le fichier a encore de l’espace, on y ajoute une page
                if (fichier.length() + config.getPagesize() <= config.getDm_maxfilesize()) {
                    try (FileOutputStream fos = new FileOutputStream(fichier, true)) {
                        fos.write(new byte[config.getPagesize()]);  // Ajoute une page vide
                        return new PageId(extractFileIndex(fichier.getName()), getNextPageIndex(fichier));
                    } catch (IOException e) {
                        System.err.println("Erreur lors de l'écriture dans le fichier " + fichier.getName() + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }

        // Si tous les fichiers sont pleins, créer un nouveau fichier
        int nextFileIndex = getNextFileIndex(fichiers);
        File newFile = new File(config.getDbpath() + "/F" + nextFileIndex + ".rsdb");
        try {
            if (newFile.createNewFile()) {
                try (FileOutputStream fos = new FileOutputStream(newFile)) {
                    fos.write(new byte[config.getPagesize()]);  // Ajoute une première page vide
                    return new PageId(nextFileIndex, 0);  // Première page du nouveau fichier
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la création du fichier : " + e.getMessage());
            e.printStackTrace();
        }

        // En cas d’échec complet, retourner null ou lever une exception personnalisée
        throw new IOException("Impossible d'allouer une page.");
    }
