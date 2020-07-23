using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkFuzzySQL.Extensions.MembershipFunction;

namespace SparkFuzzySQL.Extensions
{
    public class Class1
    {
        public void Run()
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            //var inputSchema = new StructType(new[]
            //{
            //    new StructField("age", new IntegerType()),
            //    new StructField("name", new StringType())
            //});

            //var df = spark.Read().Schema(inputSchema).Json("people.json");

            var temperature = new FuzzyVariable("Temperature");
            temperature.AddTerm(new FuzzyTerm("Warm", new TriangularMembershipFunction(15.0, 20.0, 25.0)));
            temperature.AddTerm(new FuzzyTerm("Hot", new TriangularMembershipFunction(20.0, 25.0, 30.0)));
            temperature.AddTerm(new FuzzyTerm("Holy shit I'm on fire", new TriangularMembershipFunction(25.0, 30.0, 35.0)));

            var df = spark.Sql("");

            df.FuzzyOr(temperature, new[] { "warm", "hot" }).Show();
        }
    }
}
