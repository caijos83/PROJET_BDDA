public class TestRecordID {
    public static void main(String[] args) {
        // Supposons que PageId ait un constructeur prenant deux entiers (par exemple, un identifiant de table et un identifiant de page)
        PageId pageId1 = new PageId(1, 2);
        PageId pageId2 = new PageId(3, 4);

        // Test du constructeur et des méthodes get
        RecordID recordID = new RecordID(pageId1, 5);
        System.out.println("PageId initial: " + recordID.getPageId());
        System.out.println("SlotIdx initial: " + recordID.getSlotIdx());

        // Test des méthodes set
        recordID.setPageId(pageId2);
        recordID.getSlotIdx(7);
        System.out.println("PageId après modification: " + recordID.getPageId());
        System.out.println("SlotIdx après modification: " + recordID.getSlotIdx());

        // Test de la méthode toString
        System.out.println("Affichage de RecordID : " + recordID);

        // Test d'égalité et de hashCode
        RecordID recordID2 = new RecordID(new PageId(3, 4), 7);
        RecordID recordID3 = new RecordID(new PageId(1, 2), 5);

        System.out.println("recordID est-il égal à recordID2 ? " + recordID.equals(recordID2));
        System.out.println("recordID est-il égal à recordID3 ? " + recordID.equals(recordID3));
        
        System.out.println("HashCode de recordID : " + recordID.hashCode());
        System.out.println("HashCode de recordID2 : " + recordID2.hashCode());
        System.out.println("HashCode de recordID3 : " + recordID3.hashCode());
    }
}

