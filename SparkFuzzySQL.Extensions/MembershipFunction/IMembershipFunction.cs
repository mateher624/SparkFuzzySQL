using Microsoft.Spark.Sql;

namespace SparkFuzzySQL.Extensions.MembershipFunction
{
    public interface IMembershipFunction
    {
        Column GetValueFor(Column x);
    }
}
