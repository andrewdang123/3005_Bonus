import java.util.*;

/********************************************************************
 * 
 *      Filename:       Relations.java
 *      
 *      Name:           Andrew Dang
 *      Student ID:     101297865
 *      
 *      Date:           2025-09-13
 *      
 *      Description:    After the relations get parsed, they come here and get turned into a relation object. 
 * 
 * 
 ********************************************************************/

public class Relations {
    private String name;
    private List<String> attributes;
    private List<List<String>> tuples;

/*********************************************************************************************************/

    public Relations(String name, List<String> attributes) {
        this.name = name;
        this.attributes = new ArrayList<>(attributes);
        this.tuples = new ArrayList<>();
    }

/*********************************************************************************************************/    

    public void addTuple(List<String> tuple) {
        if (tuple.size() != attributes.size()) {
            throw new IllegalArgumentException("Row does not match attribute size");
        }
        tuples.add(tuple);
    }

/*********************************************************************************************************/

    public String getName(){
        return name;
    }

/*********************************************************************************************************/    
    
    public List<String> getAttributes(){
        return attributes;
    }

/*********************************************************************************************************/    
    
    public List<List<String>> getTuples(){
        return tuples;
    }

/*********************************************************************************************************/

    public void print() {
        System.out.println("Relation: " + name);

        int cols = attributes.size();
        int[] colWidths = new int[cols];

        for (int i = 0; i < cols; i++) {
            colWidths[i] = attributes.get(i).length();
        }
        for (List<String> row : tuples) {
            for (int i = 0; i < cols; i++) {
                colWidths[i] = Math.max(colWidths[i], row.get(i).length());
            }
        }

        Runnable printBorder = () -> {
            System.out.print("+");
            for (int w : colWidths) {
                System.out.print("-".repeat(w + 2));
                System.out.print("+");
            }
            System.out.println();
        };

        printBorder.run();

        System.out.print("|");
        for (int i = 0; i < cols; i++) {
            System.out.printf(" %-"+colWidths[i]+"s |", attributes.get(i));
        }
        System.out.println();

        printBorder.run();

        for (List<String> row : tuples) {
            System.out.print("|");
            for (int i = 0; i < cols; i++) {
                System.out.printf(" %-"+colWidths[i]+"s |", row.get(i));
            }
            System.out.println();
        }

        printBorder.run();
    }

}




