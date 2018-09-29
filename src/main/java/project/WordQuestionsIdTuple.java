package project;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordQuestionsIdTuple implements Writable {
    private String word = "";
    private String stringWithListOfIds = "";

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getStringWithListOfIds() {
        return stringWithListOfIds;
    }

    public void setStringWithListOfIds(String stringWithListOfIds) {
        stringWithListOfIds = stringWithListOfIds;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(word);
        dataOutput.writeBytes(stringWithListOfIds);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readLine();
        stringWithListOfIds = dataInput.readLine();
    }

    @Override
    public String toString() {
        return "WordQuestionsIdTuple{" +
                "word='" + word + '\'' +
                ", stringWithListOfIds='" + stringWithListOfIds + '\'' +
                '}';
    }
}
