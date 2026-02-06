package net.pincette.s3.server;

import static com.typesafe.config.ConfigFactory.load;
import static java.lang.Integer.parseInt;
import static java.lang.System.exit;
import static java.util.logging.Logger.getLogger;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.isInteger;

import java.util.logging.Logger;

public class Application {
  static final Logger LOGGER = getLogger("net.pincette.s3.server");
  private static final String VERSION = "1.1.1";

  @SuppressWarnings("java:S106") // Not logging
  public static void main(final String[] args) {
    if (args.length != 1 || !isInteger(args[0])) {
      System.err.println("Usage: net.pincette.s3.server.Application port");
      exit(1);
    }

    initLogging();
    LOGGER.info(() -> "Version " + VERSION);
    new Server().withPort(parseInt(args[0])).withConfig(load()).start();
  }
}
