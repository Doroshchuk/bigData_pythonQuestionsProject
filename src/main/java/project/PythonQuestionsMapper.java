package project;

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
        ArrayListTextWritable lines = text;
        if (lines.size() < 6)
            return;
        String id = lines.get(0).toString();
        String body = lines.get(5).toString();
        body = removeHTMLFromString(body.toLowerCase()); //StringEscapeUtils.unescapeHtml(body.toLowerCase());
        body = body.replaceAll("'","");
        body = body.replaceAll("\n", " ");
        body = body.replaceAll("[^a-zA-Z]", " ");
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
