import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class WordCount {
   public static void main(String args[]) throws FileNotFoundException{
    boolean div = false;
    Scanner input;
    Map<String, Integer> wordCount = new HashMap<>();
    String path = System.getProperty("user.dir");
    File folder = new File(path);
    File[] files = folder.listFiles();
    for (File file : files) {
        if (file.isFile() && file.getName().endsWith(".html")) {
            input = new Scanner(file);
            while (input.hasNextLine()){
            String line = input.nextLine();
            if (line.matches(".*<div class=\"post_description\">.*")) {
                div = true;
            }
            if (line.trim().equals("</div>")) {
                div = false;
            }
            if (div) {
                String[] words = line.split("\s+");
                for (String word: words){
                    word = word.replaceAll("<[^>]*>", "");
                    word = word.replaceAll("[^a-zA-Z]+", "");
                    word = word.replaceAll("^href\\S*", "");
                    String wordLowerCase = word.toLowerCase();
                    if (wordCount.containsKey(wordLowerCase)) {
                        wordCount.put(wordLowerCase, wordCount.get(wordLowerCase) + 1);
                    } else {
                        wordCount.put(wordLowerCase, 1);}
                    }
                }
            }
            input.close();
        }
    }
    
    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
    }
   }
}