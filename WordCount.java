import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class WordCount {
   public static void main(String args[]) throws FileNotFoundException{
    File file = new File("problem1.txt");
    Scanner input = new Scanner(file);   
    Map<String, Integer> wordCount = new HashMap<>();
    while (input.hasNextLine()){
        String line = input.nextLine();
        String[] words = line.split("\s+");
        for (String word: words){
            word = word.toLowerCase(); 
            if (wordCount.containsKey(word)) {
                wordCount.put(word, wordCount.get(word) + 1);
            } else {
                wordCount.put(word, 1);
            }
        }
    }
    input.close();
    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
    }
   }
}