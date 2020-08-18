using Microsoft.Spark.Sql;
using SparkFuzzySQL.Extensions.MembershipFunction;

namespace SparkFuzzySQL.Extensions
{
    public class FuzzyTerm
    {
        private readonly string _name;
        private readonly IMembershipFunction _membershipFunction;

        public FuzzyTerm(string name, IMembershipFunction membershipFunction)
        {
            _name = name;
            _membershipFunction = membershipFunction;
        }

        public string GetName() => _name.ToLowerInvariant();

        public Column CalculateMembershipRatio(Column x) => _membershipFunction.GetValueFor(x);
    }
}
