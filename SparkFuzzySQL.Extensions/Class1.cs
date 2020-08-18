using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkFuzzySQL.Extensions.MembershipFunction;

namespace SparkFuzzySQL.Extensions
{
    public class Class1
    {
        public static void Run()
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var inputSchema = new StructType(new[]
            {
                new StructField("age", new IntegerType()),
                new StructField("name", new StringType()),
                new StructField("temperature", new DoubleType() ),
            });

            var secondSchema = new StructType(new[]
            {
                new StructField("temperature", new DoubleType()),
                new StructField("rating", new StringType())
            });

            var df = spark.Read().Schema(inputSchema).Json("people.json");
            var df2 = spark.Read().Schema(secondSchema).Json("ratings.json");

            var temperature = new FuzzyVariable("temperature");
            temperature.AddTerm(new FuzzyTerm("Warm", new TriangularMembershipFunction(15.0, 20.0, 25.0)));
            temperature.AddTerm(new FuzzyTerm("Hot", new TriangularMembershipFunction(20.0, 25.0, 30.0)));
            temperature.AddTerm(new FuzzyTerm("Holy shit I'm on fire", new TriangularMembershipFunction(25.0, 30.0, 35.0)));

            // var df = spark.Sql("SELECT * FROM people");

            df.FuzzyGroupBy(temperature).Count().Show();

            df.FuzzyJoin(temperature, df2).Show();

            df.Projection(temperature).Show();

            //df.FuzzyGroupBy(temperature).Show();

            df.FuzzyAnd(temperature, new[] { "warm" }).Show();
        }
    }
}
