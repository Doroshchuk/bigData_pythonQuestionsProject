package project;

import com.opencsv.CSVParser;
import in.ashwanthkumar.hadoop2.mapreduce.lib.input.ArrayListTextWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class PythonQuestionsMapper extends Mapper<LongWritable, ArrayListTextWritable, Text, Text> {
    private Text link = new Text();
    private  Text outkey = new Text();

    public void map(LongWritable key, ArrayListTextWritable text, Context context) throws IOException, InterruptedException {
        if(key.get() > 0){
            CSVParser parser = new CSVParser();

            String[] lines = parser.parseLineMulti(text.toString());
            if (lines.length == 0)
                return;
            String id = lines[0].replace("[", "");
            String body = lines[lines.length - 1].replace("]", "");
            body = removeHTMLFromString(body.toLowerCase()); //StringEscapeUtils.unescapeHtml(body.toLowerCase());
            body = body.replaceAll("'","");
            body = body.replaceAll("\n", " ");
            body = body.replaceAll("[^a-zA-Z]", " ");
            System.out.println("Id: " + id);
            System.out.println("Body: " + body);
            StringTokenizer itr = new StringTokenizer(body, ".,?\n\\s");
            while(itr.hasMoreTokens()){
                String word = itr.nextToken();
                if (word.charAt(0) == 'j') {
                    link.set(word);
                    outkey.set(id);
                    context.write(link, outkey);
                }
            }
        }
    }

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
