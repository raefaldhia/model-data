package io.github.raefaldhia;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void
    main (String[] args) {    
        // Disable MongoDB logger
	System.out.println();
        Logger
            .getLogger("org.mongodb.driver")
            .setLevel(Level.OFF);
    }
}
