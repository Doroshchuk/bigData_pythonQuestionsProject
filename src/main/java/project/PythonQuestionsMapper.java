package project;

import com.opencsv.CSVParser;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class PythonQuestionsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text link = new Text();
    private  Text outkey = new Text();

    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        if(key.get() > 0){
            CSVParser parser = new CSVParser();

            String[] lines = parser.parseLineMulti(text.toString());
            if (lines.length == 0)
                return;
            String id = lines[0];
            String body = lines[lines.length - 1];
            System.out.println("Id: " + id);
            System.out.println("Body: " + body);
            body = StringEscapeUtils.unescapeHtml(body.toLowerCase());
            //removeHTMLFromString(body.toLowerCase());
            //body = body.replaceAll("'","");
            //body = body.replaceAll("[^a-zA-Z]]", " ");
            //body.replaceAll("'","").replaceAll("\n", " ").replaceAll("[^a-zA-Z]", " ");
            StringTokenizer itr = new StringTokenizer(body);
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
