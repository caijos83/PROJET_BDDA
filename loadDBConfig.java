

public static DBConfig loadDBConfig(String filePath) throws IOException {
        System.out.println(System.getProperty("user.dir"));
        String line;
        String dbpath = null;
        int pagesize = 0;
        long dm_maxfilesize = 0;
        String bm_policy = null;
        int bm_buffercount = 0;
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        try {
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                switch (parts[0].trim()) {
                    case "dbpath":
                        dbpath = parts[1].trim();
                        // trim() est utilisé pour enlever les espaces blancs en début et fin de chaîne, assurant que les clés et valeurs sont correctement interprétées.
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
            reader.close();
        } catch (FileNotFoundException e) {
            // Gestion de l'exception : fichier introuvable
            System.out.println("Fichier introuvable: " + filePath);
        }
        catch (IOException e){
            // Gestion de l'exception
            e.printStackTrace();
        }finally {
            // fermeture du flux
            reader.close();
        }
        return new DBConfig(dbpath, pagesize, dm_maxfilesize, bm_policy, bm_buffercount);
    }
