import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.function.BiPredicate;

/********************************************************************
 * 
 *      Filename:       Main.java
 *      
 *      Name:           Andrew Dang
 *      Student ID:     101297865
 *      
 *      Date:           2025-09-13
 *      
 *      Description:    This is where the relations get parsed and sent into an object
 *                      After that, the query gets processed, and a new object is created based on the query
 * 
 * 
 *      Operations      select          a = b (A)               Supports >, <, =, !=
 *                      project         a, b (A)
 *                      cjoin           (A) (B)
 *                      ijoin           (A) (B) A.a = B.b
 *                      njoin           (A) (B)
 *                      intersect       (A) (B)
 *                      union           (A) (B)
 *                      minus           (A) (B)
 *                      
 *                      
 ********************************************************************/

public class Main {

    public static Relations findOperation(String query, Map<String, Relations> database){
        // Get the first word of the query to determine what operation it is
        int firstSpace = query.indexOf(" ");
        String operation = query.substring(0, firstSpace);
        String rest = query.substring(firstSpace + 1);

        if (operation.equalsIgnoreCase("select")){
            return select(rest, database);
        }
        else if (operation.equalsIgnoreCase("project")){
            return project(rest, database);
        }
        else if (operation.equalsIgnoreCase("cjoin")){
            return cjoin(rest, database);
        }
        else if (operation.equalsIgnoreCase("ijoin")){
            return ijoin(rest, database);
        }
        else if (operation.equalsIgnoreCase("njoin")){
            return njoin(rest, database);
        }
        else if (operation.equalsIgnoreCase("intersection")){
            return intersection(rest, database);
        }
        else if (operation.equalsIgnoreCase("union")){
            return union(rest, database);
        }
        else if (operation.equalsIgnoreCase("minus")){
            return minus(rest, database);
        }

        throw new IllegalArgumentException("Not a valid Query");
        
    }

/*********************************************************************************************************/
    
    public static boolean isInteger(String str) {

        // Helper function for intersect, union, minus
        if (str == null || str.isEmpty()) return false;
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

/*********************************************************************************************************/

    public static boolean sameDataTypes(Relations r1, Relations r2) {
        // Another helper function for intersect, union, minus
        // Check size
        if (r1.getAttributes().size() != r2.getAttributes().size()){
            return false;
        }

        List<List<String>> tuple1 = r1.getTuples();
        List<List<String>> tuple2 = r2.getTuples();
        // Check for the actual data type
        for (int i = 0; i < r1.getAttributes().size(); i++){
            if (isInteger(tuple1.get(0).get(i)) != isInteger(tuple2.get(0).get(i))){
                return false;
            }
        }

        return true;
    }

/*********************************************************************************************************/

    public static Relations getRelation(String relationName, Map<String, Relations> database){
        // Either get the relation
        if (database.containsKey(relationName)){
            return database.get(relationName);
        }
        // It's not a relation, but a nested query
        else{
            return findOperation(relationName, database);
        }

    }

/*********************************************************************************************************/

    public static Relations select(String argument, Map<String, Relations> database){

        int space;
        String operand[] = new String[3];

        // Getting the three parts of the argument, i.e.,
        // [Age, <, 30]
        for (int i = 0; i<3; i++){
            space = argument.indexOf(" ");
            operand[i] = argument.substring(0, space);
            argument = argument.substring(space+1);
        }
        
        argument = argument.substring(1,argument.length()-1);
        Relations relation = getRelation(argument, database);
        List<String> attributes = relation.getAttributes();

        Relations result = new Relations("Selection: " + operand[0]+ " " + operand[1]+ " " + operand[2], attributes);
        
        // Ensuring that the first word is a valid attribute
        int column = -1;
        for (int i = 0; i < attributes.size(); i++){
            if (attributes.get(i).equalsIgnoreCase(operand[0])){
                column = i;
                break;
            }
        }

        if (column == -1){
            throw new IllegalArgumentException("Not a valid Query");
        }

        // Way to switch out the operator symbol
        // Check for int with < >
        Map<String, BiPredicate<Integer, Integer>> operators1 = new HashMap<>();
        operators1.put(">", (a, b) -> a > b);
        operators1.put("<", (a, b) -> a < b);
        // String for equal and does not equal
        Map<String, BiPredicate<String, String>> operators2 = new HashMap<>();
        operators2.put("=", (a, b) -> a.equals(b));
        operators2.put("!=", (a, b) -> !a.equals(b));

        List<List<String>> tuples = relation.getTuples();

        // Here I check if row passes the condition
        for (int i = 0; i < tuples.get(0).size(); i++){
            if (operand[1].equals("<") || operand[1].equals(">")){
                if (operators1.get(operand[1]).test(Integer.parseInt(tuples.get(i).get(column)), Integer.parseInt(operand[2]))){
                    result.addTuple(tuples.get(i));
                }
            }
            else{
                if (operators2.get(operand[1]).test(tuples.get(i).get(column), operand[2])){
                    result.addTuple(tuples.get(i));
                }
            }
            
        }

        return result;
    }

/*********************************************************************************************************/

    public static Relations project(String argument, Map<String, Relations> database){
        // Get all the attributes that need to be projected
        String beforeParen = argument.split("\\(")[0].trim();
        List<String> newAttributes = Arrays.asList(beforeParen.replace(",", "").split("\\s+"));

        // Gets the relation name
        int start = argument.indexOf("(");
        int end = argument.lastIndexOf(")");
        String table = "";
        if (start != -1 && end != -1 && end > start) {
            table = argument.substring(start + 1, end).trim();
        }

        Relations relation = getRelation(table, database);

        List<String> attributes = relation.getAttributes();
        Relations result = new Relations("Projections: " + newAttributes.toString(), newAttributes);
        
        // Finds the indexes of the attributes to project
        Set toProject = new HashSet();
        for (int i = 0; i < attributes.size(); i++){
            if (newAttributes.contains(attributes.get(i))){
                toProject.add(i);
            }
        }
 
        List<List<String>> tuples = relation.getTuples();
        
        for (int i = 0; i < tuples.size();i++){
            List<String> toAdd = new ArrayList<>();
            for (int j = 0; j < attributes.size(); j++){
                // If it was listed, add it to the tuple
                if (toProject.contains(j)){
                    toAdd.add(tuples.get(i).get(j));
                }
            }
            result.addTuple(toAdd);
        }
        
        return result;
    }

/*********************************************************************************************************/

    public static Relations cjoin(String argument, Map<String, Relations> database){
        // More string stuff
        // Getting the relations within the brackets
        int splitIndex = argument.indexOf(") (");
        String string1 = argument.substring(1, splitIndex); 
        String string2 = argument.substring(splitIndex + 3, argument.length() - 1);
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);

        List<String> attributes = new ArrayList<>(relation1.getAttributes());
        attributes.addAll(relation2.getAttributes());

        Relations result = new Relations("Cross Join: (" + string1 + ") (" + string2 + ")", attributes);

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        
        // Nested for loop which just adds the second relation row onto the first
        for (List<String> tuple_1 : tuple1) {
            for (List<String> tuple_2 : tuple2) {
                List<String> toAdd = new ArrayList<>();
                toAdd.addAll(tuple_1);
                toAdd.addAll(tuple_2);
                result.addTuple(toAdd);
            }
        }
        return result;
    }

/*********************************************************************************************************/

    public static Relations ijoin(String argument, Map<String, Relations> database){
        // First get the relations in brackets
        int firstParenEnd = argument.indexOf(")");
        String string1 = argument.substring(1, firstParenEnd);

        int secondParenStart = argument.indexOf("(", firstParenEnd);
        int secondParenEnd = argument.indexOf(")", secondParenStart);
        String string2 = argument.substring(secondParenStart + 1, secondParenEnd);

        String condition = argument.substring(secondParenEnd + 2).trim(); 

        String[] parts = condition.split("=");

        String attr1 = parts[0].trim(); 
        String attr2 = parts[1].trim();
        // Get the Attributes of the relations after the .
        attr1 = attr1.substring(attr1.indexOf(".") + 1); 
        attr2 = attr2.substring(attr2.indexOf(".") + 1);   
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);

        List<String> attributes = new ArrayList<>(relation1.getAttributes());
        attributes.addAll(relation2.getAttributes());

        String title = "(" + string1 + ") (" + string2 + ") " + string1 + "." + attr1 + " = " + string2 + "." + attr2;
        Relations result = new Relations("Inner Join: " + title, attributes);

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        
        int[] toCompare = new int[2];
        boolean valid1 = false;
        boolean valid2 = false;
        // These two loops are meant to find the index of the attributes
        for (int i = 0; i < relation1.getAttributes().size(); i++){
            if (attr1.equalsIgnoreCase(relation1.getAttributes().get(i))){
                toCompare[0] = i;
                valid1 = true;
                break;
            }
        }
        for (int i = 0; i < relation2.getAttributes().size(); i++){
            if (attr2.equalsIgnoreCase(relation2.getAttributes().get(i))){
                toCompare[1] = i;
                valid2 = true;
                break;
            }
        }

        if (!valid1 && !valid2){
            throw new IllegalArgumentException("Attributes not found");
        }
        // Almost same as what i did in cross product
        for (List<String> tuple_1 : tuple1) {
            for (List<String> tuple_2 : tuple2) {
                // Check to see if attribute 1 is equal to attribute 2
                if (tuple_1.get(toCompare[0]).equals(tuple_2.get(toCompare[1]))){
                    List<String> toAdd = new ArrayList<>();
                    toAdd.addAll(tuple_1);
                    toAdd.addAll(tuple_2);
                    result.addTuple(toAdd);
                }
            }
        }
        return result;
    }

/*********************************************************************************************************/

    public static Relations njoin(String argument, Map<String, Relations> database){
        // Get the stuff in the brackets
        int splitIndex = argument.indexOf(") (");

        String string1 = argument.substring(1, splitIndex); 
        String string2 = argument.substring(splitIndex + 3, argument.length() - 1);
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);
        
        List<String> attributes = new ArrayList<>(relation1.getAttributes());
        attributes.addAll(relation2.getAttributes());

        Relations result = new Relations("Natural Join: (" + string1 + ") (" + string2 + ")", attributes);

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        
        int[] toCompare = new int[2];
        boolean valid = false;
        // Finds if attributes are the same
        for (int i = 0; i < relation1.getAttributes().size(); i++){
            if (relation2.getAttributes().contains(relation1.getAttributes().get(i))){
                toCompare[0] = i;
                valid = true;
                break;
            }
        }
        if (!valid){
            return result;
        }
        // Finds the indexes of the same attribute
        for (int i = 0; i < relation2.getAttributes().size(); i++){
            if (relation2.getAttributes().get(i).equals(relation1.getAttributes().get(toCompare[0]))){
                toCompare[1] = i;
                break;
            }
        }
        // Essentially the same code as above in inner join
        for (List<String> tuple_1 : tuple1) {
            for (List<String> tuple_2 : tuple2) {
                if (tuple_1.get(toCompare[0]).equals(tuple_2.get(toCompare[1]))){
                    List<String> toAdd = new ArrayList<>();
                    toAdd.addAll(tuple_1);
                    toAdd.addAll(tuple_2);
                    result.addTuple(toAdd);
                }
            }
        }
        return result;
    }
    
/*********************************************************************************************************/

    public static Relations intersection(String argument, Map<String, Relations> database){
        // Getting the stuff in the brackets
        int splitIndex = argument.indexOf(") (");

        String string1 = argument.substring(1, splitIndex); 
        String string2 = argument.substring(splitIndex + 3, argument.length() - 1);
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);
        // Checks attributes types
        if (!sameDataTypes(relation1, relation2)){
            throw new IllegalArgumentException("Attributes not the same");
        }

        Relations result = new Relations("Intersection: (" + string1 + ") (" + string2 + ")", relation1.getAttributes());

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        //Checks if the row exists in the second relation
        // if so adds it
        for (List<String> tuple : tuple1) {
            if (tuple2.contains(tuple)) {
                result.addTuple(tuple);
            }
        }

        return result;
    }

/*********************************************************************************************************/

    public static Relations union(String argument, Map<String, Relations> database){
        // Checks inside the brackets for relationns
        int splitIndex = argument.indexOf(") (");

        String string1 = argument.substring(1, splitIndex); 
        String string2 = argument.substring(splitIndex + 3, argument.length() - 1);
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);
        // Checks the attributes again
        if (!sameDataTypes(relation1, relation2)){
            throw new IllegalArgumentException("Attributes not the same");
        }

        Relations result = new Relations("Union: (" + string1 + ") (" + string2 + ")", relation1.getAttributes());

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        // Adds all from the first relation
        for (List<String> tuple : tuple1) {
            result.addTuple(tuple);
        }
        // Checks for new rows not in the first relation
        for (List<String> tuple : tuple2) {
            if (!tuple1.contains(tuple)) {
                result.addTuple(tuple);
            }
        }

        return result;
    }

/*********************************************************************************************************/

    public static Relations minus(String argument, Map<String, Relations> database){
        // Getting the relations
        int splitIndex = argument.indexOf(") (");

        String string1 = argument.substring(1, splitIndex); 
        String string2 = argument.substring(splitIndex + 3, argument.length() - 1);
        
        Relations relation1 = getRelation(string1, database);
        Relations relation2 = getRelation(string2, database);
        // Ensuring the attributes are the same
        if (!sameDataTypes(relation1, relation2)){
            throw new IllegalArgumentException("Attributes not the same");
        }

        Relations result = new Relations("Minus: (" + string1 + ") (" + string2 + ")", relation1.getAttributes());

        List<List<String>> tuple1 = relation1.getTuples();
        List<List<String>> tuple2 = relation2.getTuples();
        // Checks to see if row does not exist in second relation, if so add it
        for (List<String> tuple : tuple1) {
            if (!tuple2.contains(tuple)) {
                result.addTuple(tuple);
            }
        }


        return result;
    }

/*********************************************************************************************************/

    public static void main(String[] args) throws IOException {
        Map<String, Relations> database = new HashMap<>(); 
        String query = "";
        //Change this part for different files
        InputStream is = Main.class.getResourceAsStream("/data/projection.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        Relations currentRelation = null;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            
            // Header
            if (line.contains("(") && line.contains(")") && line.contains("{")) {
                String[] parts = line.split("\\(|\\)|=");
                String name = parts[0].trim();
                String attrs = parts[1].trim();
                List<String> attributes = Arrays.stream(attrs.split(","))
                                                .map(String::trim)
                                                .toList();
                currentRelation = new Relations(name, attributes);
                database.put(name, currentRelation);
            } 
            
            else if (line.startsWith("{") || line.endsWith("}")) {
                continue;
            } 
            else if(line.toUpperCase().startsWith("QUERY")){
                query = line.substring(6).trim();
            }
            else {
                List<String> values = Arrays.stream(line.split(","))
                                            .map(String::trim)
                                            .toList();
                currentRelation.addTuple(values);
            }
        }
        br.close();

        System.out.println("Current Relation(s):\n");
        for (Relations rel : database.values()) {
            rel.print();
            System.out.println("\n");
        }
        Relations result = findOperation(query, database);
        System.out.println("Queried Relation:\n");
        result.print();

    }

}
