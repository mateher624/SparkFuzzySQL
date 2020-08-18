using System;
using System.Diagnostics;

namespace SparkFuzzySQL.Runner
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Runner enabled");

            var cmd = new Process
            {
                StartInfo =
                {
                    FileName = "cmd.exe",
                    Arguments = "/C \" spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local microsoft-spark-2.4.x-0.12.1.jar dotnet D:\\Users\\Mateusz\\Sources\\SparkFuzzySQL\\SparkFuzzySQL.Worker\\bin\\Debug\\netcoreapp3.1\\SparkFuzzySQL.Worker.dll \"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    Verb = "runas",
                    CreateNoWindow = true,
                    UseShellExecute = false,
                }
            };

            cmd.OutputDataReceived += OutputDataReceived;
            cmd.ErrorDataReceived += ErrorDataReceived;

            cmd.Start();

            cmd.BeginOutputReadLine();
            cmd.BeginErrorReadLine();


            cmd.WaitForExit();
        }

        private static void OutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            Console.WriteLine(e.Data);
        }

        private static void ErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            //Console.WriteLine(e.Data);
        }
    }
}
