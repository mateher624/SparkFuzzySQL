using System;
using Microsoft.Spark.Sql;
using SparkFuzzySQL.Extensions;

namespace SparkFuzzySQL.Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("Worker enabled");

            Class1.Run();

        }
    }
}
