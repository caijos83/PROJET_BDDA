//package projet_SGBD;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"
    private PageId headerPageId; 
    private DiskManager dm;
    private BufferManager bm;

    private List<PageId> pageDirectory;


    public Relation(String name,PageId headerPageId, DiskManager dm, BufferManager bm) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.headerPageId = headerPageId;
        this.dm = dm;
        this.bm = bm;

        this.pageDirectory = new ArrayList<>();
    }
  
    public void addColumn(String name, String type) {
        columnNames.add(name);
        columnTypes.add(type);
    }

    public int writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);
        int totalSize = 0;
        for (int i = 0; i < record.size(); i++) {
            Object value = record.getValue(i);
            String type = columnTypes.get(i);
            if (type.equals("INT")) {
                buff.putInt((Integer) value);
                totalSize += Integer.BYTES;
            } else if (type.equals("REAL")) {
                buff.putFloat((Float) value);
                totalSize += Float.BYTES;
            } else if (type.startsWith("CHAR")) {
                int length = Integer.parseInt(type.substring(5, type.length() - 1)); // Extrait T
                String strValue = (String) value;
                for (char c : strValue.toCharArray()) {
                    buff.put((byte) c);
                }
                for (int j = strValue.length(); j < length; j++) {
                    buff.put((byte) 0); // Caractère nul
                }
                totalSize += length;
            } else if (type.startsWith("VARCHAR")) {
                String strValue = (String) value;
                buff.putInt(strValue.length()); // Longueur de la chaîne
                for (char c : strValue.toCharArray()) {
                    buff.put((byte) c);
                }
                totalSize += 4 + strValue.length(); // 4 octets pour la longueur + longueur de la chaîne
            }
        }

        return totalSize;
    }
    
    

    public int readFromBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);
        int totalSize = 0;

        for (String type : columnTypes) {
            if (type.equals("INT")) {
                record.addValue(buff.getInt());
                totalSize += Integer.BYTES;
            } else if (type.equals("REAL")) {
                record.addValue(buff.getFloat());
                totalSize += Float.BYTES;
            } else if (type.startsWith("CHAR")) {
                int length = Integer.parseInt(type.substring(5, type.length() - 1));
                byte[] charBytes = new byte[length];
                buff.get(charBytes);
                String strValue = new String(charBytes).replaceAll("\u0000$", ""); // Supprime les caractères nuls
                record.addValue(strValue);
                totalSize += length;
            } else if (type.startsWith("VARCHAR")) {
                int strLength = buff.getInt();
                byte[] strBytes = new byte[strLength];
                buff.get(strBytes);
                String strValue = new String(strBytes);
                record.addValue(strValue);
                totalSize += 4 + strLength; // 4 pour la longueur + longueur de la chaîne
            }
        }

        return totalSize;
    }
    
    public int getColumnNumber() {
        return columnNames.size();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }public PageId getHeaderPageId() {
        return headerPageId;
    }

    public DiskManager getDiskManager() {
        return dm;
    }

    public BufferManager getBufferManager() {
        return bm;
    }

    public void addDataPage() throws IOException {
       
        PageId newPageId = dm.allocPage(); //Allouer une nouvelle page via DiskManager
        pageDirectory.add(newPageId);
        System.out.println("Nouvelle page de données ajoutée avec PageId : " + newPageId);
    }
    public PageId getFreeDataPageId( int sizeRecord)throws IOException {
        for (PageId pageId : pageDirectory) {
            ByteBuffer buffer = bm.getPage(pageId);         
            int freeSpace = buffer.capacity() - buffer.position();// Vérifie si le ByteBuffer a assez d'espace pour le record
            if (freeSpace >= sizeRecord) {
                return pageId; // Retourne la première page avec assez de place
            }
        }
        // Retourne null si dépassement 
        return null;
    }

    public RecordId writeRecordToDataPage(Record record, PageId pageId) throws IOException {

        ByteBuffer buffer = bm.getPage(pageId);
        int pos = buffer.position();
        int bytesWritten = writeRecordToBuffer(record, buffer, pos);

        int slotIdx = pos / bytesWritten;
        return new RecordId(pageId, slotIdx);
    }


    public List<Record> getRecordsInDataPage(PageId pageId) throws IOException {

        List<Record> records = new ArrayList<>();
        ByteBuffer buffer = bm.getPage(pageId);
        buffer.position(0);

        // Lire chaque enregistrement dans la page
        while (buffer.remaining() > 0) {
            Record record = new Record();
            int bytesRead = readFromBuffer(record, buffer, buffer.position());

            // Si bytesRead est 0, cela signifie qu'il n'y a plus de records valides
            if (bytesRead == 0) break;
            // Ajouter le record à la liste
            records.add(record);
        }

        // Libérer la page après la lecture
        bm.FreePage(pageId,);

        return records;
    }

    public List<PageId> getDataPages() throws IOException {
        List<PageId> dataPages = new ArrayList<>();

        // Récupérer la page d'en-tête qui contient les PageIds des pages de données
        ByteBuffer headerBuffer = bm.getPage(headerPageId);

        // Suppose que la liste des PageIds des pages de données commence à un endroit spécifique de la Header Page.
        // (Cette partie peut varier selon la façon dont la Header Page est structurée)
        headerBuffer.position(0);
        while (headerBuffer.remaining() > 0) {
            int pageIdValue = headerBuffer.getInt();
            PageId pageId = new PageId(pageIdValue,);
            dataPages.add(pageId);
        }

        // Libérer la page d'en-tête après lecture
        bm.freePage(headerPageId,);

        return dataPages;
    }

    // Méthode pour insérer un Record
    public RecordId InsertRecord(Record record) throws IOException {
        // Calculer la taille du record à insérer
        int sizeRecord = getRecordSize(record);
        
        // Obtenir le PageId de la première page disponible ayant assez d'espace
        PageId freePageId = getFreeDataPageId(sizeRecord);

        if (freePageId == null) {
            // Si aucune page disponible, on doit en ajouter une nouvelle
            addDataPage();
            freePageId = pageDirectory.get(pageDirectory.size() - 1); // La dernière page ajoutée
        }

        // Écrire le record dans la page et obtenir le RecordId
        RecordId rid = writeRecordToDataPage(record, freePageId);

        return rid;
    }

    // Méthode pour récupérer tous les records
    public List<Record> GetAllRecords() throws IOException {
        List<Record> allRecords = new ArrayList<>();
        
        // Récupérer les PageIds des pages de données
        List<PageId> dataPages = getDataPages();

        // Parcourir toutes les pages de données et récupérer les records
        for (PageId pageId : dataPages) {
            List<Record> recordsInPage = getRecordsInDataPage(pageId);
            allRecords.addAll(recordsInPage);
        }

        return allRecords;
    }

    // Supposons que cette méthode calcule la taille du record
    private int getRecordSize(Record record) {
        // Calculer la taille du record en fonction de ses champs (en fonction des types de données)
        int size = 0;
        for (int i = 0; i < record.size(); i++) {
            String type = columnTypes.get(i);
            Object value = record.getValue(i);
            if (type.equals("INT")) {
                size += Integer.BYTES;
            } else if (type.equals("REAL")) {
                size += Float.BYTES;
            } else if (type.startsWith("CHAR")) {
                int length = Integer.parseInt(type.substring(5, type.length() - 1)); // Extrait T
                size += length;
            } else if (type.startsWith("VARCHAR")) {
                String strValue = (String) value;
                size += 4 + strValue.length(); // 4 octets pour la longueur + longueur de la chaîne
            }
        }
        return size;
    }

}
    