package io.github.raefaldhia;

import io.github.raefaldhia.data.DataGenerator;
import java.io.IOException;

public class Main {
    public static void main(final String[] args) throws IOException {
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.generateData();
        return;
    }
}
