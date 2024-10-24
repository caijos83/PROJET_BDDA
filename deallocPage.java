public void deallocPage(PageId pageId) {
        // Parcourir les fichiers existants pour trouver celui correspondant à pageId
        File dbDir = new File(config.getDbpath());
        File[] fichiers = dbDir.listFiles((dir, name) -> name.endsWith(".rsdb"));

        boolean pageFound = false; // Indicateur pour savoir si la page a été trouvée

        if (fichiers != null) {
            for (File fichier : fichiers) {
                // Vérifie si le fichier correspond à celui du pageId
                if (extractFileIndex(fichier.getName()) == pageId.getFileIdx()) {
                    // Ici, on pourrait envisager de marquer la page dans le fichier ou gérer d'autres opérations
                    // Actuellement, nous ajoutons simplement le PageId à la liste des pages libres

                    freePages.add(pageId); // Ajoute le PageId à la liste des pages libres
                    pageFound = true; // Marque que la page a été trouvée et désallouée

                    System.out.println("Page " + pageId + " désallouée.");
                    break; // Sort de la boucle après avoir désalloué la page
                }
            }
        }

        // Gérer le cas où la page n'a pas été trouvée
        if (!pageFound) {
            System.err.println("Aucune page trouvée pour la désallocation : " + pageId);
        }
    }
