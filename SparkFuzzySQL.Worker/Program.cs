using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using SparkFuzzySQL.Extensions;
using SparkFuzzySQL.Extensions.MembershipFunction;

namespace SparkFuzzySQL.Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            var peopleSchema = new StructType(new[]
            {
                new StructField("name", new StringType()),
                new StructField("age", new IntegerType())
            });

            var salarySchema = new StructType(new[]
            {
                new StructField("age", new IntegerType()),
                new StructField("salary", new DoubleType())
            });

            var weatherSchema = new StructType(new[]
            {
                new StructField("date", new StringType()),
                new StructField("temperature", new DoubleType())
            });

            var ratingsSchema = new StructType(new[]
            {
                new StructField("temperature", new DoubleType()),
                new StructField("rating", new StringType())
            });

            var peopleData = spark.Read().Schema(peopleSchema).Json("people.json");
            var salaryData = spark.Read().Schema(salarySchema).Json("salary.json");
            var weatherData = spark.Read().Schema(weatherSchema).Json("weather.json");
            var ratingsData = spark.Read().Schema(ratingsSchema).Json("ratings.json");


            peopleData.Show(40);
            salaryData.Show(40);
            weatherData.Show(40);
            ratingsData.Show(40);

            TestMethodsTemperature(weatherData, ratingsData);
            TestMethodsAge(peopleData, salaryData);

        }

        private static void TestMethodsTemperature(DataFrame df, DataFrame df2)
        {
            var temperature = DefineTemperatureVariable();

            df.FuzzyAnd(temperature, new[] {"chilly", "warm"}).Show();

            df.FuzzyOr(temperature, new[] {"chilly", "warm"}).Show();

            df.FuzzySelect(temperature, "hot").Show();

            df.FuzzySelect(temperature, "hot", 0.01).Show();

            df.Projection(temperature).Show();

            df.FuzzyGroupBy(temperature).Count().Show();

            df.FuzzyGroupBy(temperature).Avg(temperature.GetName()).Show();

            df.FuzzyJoin(temperature, df2).Show(40);
        }

        private static void TestMethodsAge(DataFrame df, DataFrame df2)
        {
            var age = DefineAgeVariable();

            df.FuzzyAnd(age, new[] { "adult", "middle-aged" }).Show();

            df.FuzzyOr(age, new[] { "adult", "middle-aged" }).Show();

            df.FuzzySelect(age, "adolescent").Show();

            df.FuzzySelect(age, "adolescent", 0.01).Show();

            df.Projection(age).Show(40);

            df.FuzzyGroupBy(age).Count().Show();

            df.FuzzyGroupBy(age).Avg(age.GetName()).Show();

            df.FuzzyJoin(age, df2).Show(40);

            df.FuzzyJoin(age, df2).GroupBy("name").Avg("salary").Show(40);
        }

        static FuzzyVariable DefineTemperatureVariable()
        {
            var temperature = new FuzzyVariable("temperature");
            temperature.AddTerm(new FuzzyTerm("freezing", new TriangularMembershipFunction(5.0, 10.0, 15.0)));
            temperature.AddTerm(new FuzzyTerm("cold", new TriangularMembershipFunction(10.0, 15.0, 20.0)));
            temperature.AddTerm(new FuzzyTerm("chilly", new TriangularMembershipFunction(15.0, 20.0, 25.0)));
            temperature.AddTerm(new FuzzyTerm("warm", new TriangularMembershipFunction(20.0, 25.0, 30.0)));
            temperature.AddTerm(new FuzzyTerm("hot", new TriangularMembershipFunction(25.0, 30.0, 35.0)));
            return temperature;
        }

        static FuzzyVariable DefineAgeVariable()
        {
            var age = new FuzzyVariable("age");
            age.AddTerm(new FuzzyTerm("child", new TrapezoidMembershipFunction(0.0, 0.0, 10.0,14.0)));
            age.AddTerm(new FuzzyTerm("adolescent", new TrapezoidMembershipFunction(10.0, 13.0, 17.0, 20.0)));
            age.AddTerm(new FuzzyTerm("adult", new TrapezoidMembershipFunction(17.0, 21.0, 36.0,40.0)));
            age.AddTerm(new FuzzyTerm("middle-aged", new TrapezoidMembershipFunction(36.0, 40.0, 48.0, 52.0)));
            age.AddTerm(new FuzzyTerm("aged", new TrapezoidMembershipFunction(48.0, 60.0, 80.0, 80.0)));
            return age;
        }
    }
}
