package project;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PythonQuestionsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text link = new Text();
    private  Text outkey = new Text();
    private static int count = 0;

    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser();
            String[] lines = parser.parseLineMulti(text.toString());
            if (lines.length == 0)
                return;
            String id = lines[0];
            String body = lines[lines.length - 1];
            String rawBody = removeHTMLFromString(body.toLowerCase());
            String bodyChars = rawBody.replaceAll("'","").replaceAll("\n", " ").replaceAll("[^a-zA-Z]", " ");
            if(count < 5 && lines.length == 5){
                System.out.println("Id: " + id);
                System.out.println("OwnerUserId: " + lines[1]);
                System.out.println("CreationDate: " + lines[2]);
                System.out.println("Score: " + lines[3]);
                System.out.println("Title: " + lines[4]);
                System.out.println("Body: " + bodyChars);
            }
            count ++;
            for (String word: bodyChars.split("\\s+")) {
//                System.out.println("Id: " + id);
//                System.out.println("Word: " + word);
                if (word.length() > 0) {
                    if (word.charAt(0) == 'j') {
                        link.set(word);
                        outkey.set(id);
                        context.write(link, outkey);
                    }
                }
            }
    }

//    public void map(Text text) throws IOException, InterruptedException {
//        CSVParser parser = new CSVParser();
//        String[] lines = parser.parseLineMulti(text.toString());
//        if (lines.length == 0)
//            return;
//        String id = lines[0];
//        String body = lines[5];
//        String rawBody = removeHTMLFromString(body.toLowerCase());
//        String bodyChars = rawBody.replaceAll("'","").replaceAll("\n", " ").replaceAll("[^a-zA-Z]", " ");
//        for (String word: bodyChars.split("\\s+")) {
//            System.out.println("Id: " + id);
//            System.out.println("Word: " + word);
//            if (word.length() > 0) {
//                if (word.charAt(0) == 'j') {
//                    link.set(word);
//                    outkey.set(id);
//                }
//            }
//        }
//    }

    private String removeHTMLFromString(String textWithTML){
        String strRegEx = "<[^>]*>";
        String textWithoutHTML = textWithTML.replaceAll(strRegEx, "");

        //replace &nbsp; with space
        textWithoutHTML = textWithoutHTML.replace("&nbsp;", " ");
        //replace &amp; with &
        textWithoutHTML = textWithoutHTML.replace("&amp;", "&");

        //OR remove all HTML entities
        textWithoutHTML = textWithoutHTML.replaceAll("&.*?;", "");
        return textWithoutHTML;
    }
}
