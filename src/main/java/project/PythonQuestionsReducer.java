package project;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PythonQuestionsReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirstElement = true;
        for (Text id : values){
            if (isFirstElement){
                isFirstElement = false;
            } else stringBuilder.append(" ");
            stringBuilder.append(id.toString());
        }

        result.set(stringBuilder.toString());
        context.write(new Text("Id: " + key), new Text("Word: " + result));
    }
}
