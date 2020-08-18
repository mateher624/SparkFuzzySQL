using SyslogLogging;

namespace SparkFuzzySQL.Extensions
{
    public static class Logger
    {
        private static readonly LoggingModule Logging;

        static Logger()
        {
            Logging = new LoggingModule("127.0.0.1", 514)
            {
                ConsoleEnable = true, 
                FileLogging = FileLoggingMode.FileWithDate, 
                LogFilename = "D:\\Users\\Mateusz\\Sources\\SparkFuzzySQL\\SparkFuzzySQL.Worker\\bin\\Debug\\netcoreapp3.1\\log.txt"

            };
            // or Disabled, or SingleLogFile
        }

        public static void Info(string message)
        {
            Logging.Info(message);
        }
    }
}
