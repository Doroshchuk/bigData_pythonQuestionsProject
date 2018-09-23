import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PythonQuestionsMapper extends Mapper<Object, Text, Text, Text> {
    private Text link = new Text();
    private  Text outkey = new Text();

    public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
        CSVParser csvParser = CSVParser.parse(text.toString(), CSVFormat.DEFAULT
                .withHeader("Id", "OwnerUserId", "CreationDate", "Score", "Title", "Body")
                .withIgnoreHeaderCase()
                .withTrim());

        for (CSVRecord csvRecord: csvParser) {
            String id = csvRecord.get("Id");
            String body = csvRecord.get("Body");

            body = StringEscapeUtils.escapeHtml(body.toLowerCase());

            for (String word: body.split("\\s+")) {
                if (word.charAt(0) == 'j'){
                    link.set(word);
                    outkey.set(id);
                    context.write(link, outkey);
                }
            }
        }
    }
}
