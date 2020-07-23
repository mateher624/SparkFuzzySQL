using System;
using Microsoft.Spark.Sql;

namespace SparkFuzzySQL.Extensions.MembershipFunction
{
    public class TrapezoidMembershipFunction : IMembershipFunction
    {
        public double A { get; set; }
        public double B { get; set; }
        public double C { get; set; }
        public double D { get; set; }

        public TrapezoidMembershipFunction(double a, double b, double c, double d)
        {
            if (!(a <= b && b <= c && c <= d))
            {
                throw new ArgumentException("Incorrect relationship between the trapeze coordinates");
            }

            A = a;
            B = b;
            C = c;
            D = d;
        }

        public double GetValueFor(double x)
        {
            if (x == A && x == B || x == C && x == D)
                return 1.0;

            if (x <= A || x >= D)
                return 0.0;

            if ((x >= B) && (x <= C))
                return 1.0;

            if (x > A && x < B)
                return x / (B - A) - A / (B - A);

            return -x / (D - C) + D / (D - C);
        }

        public Column GetValueFor(Column x)
        {
            throw new NotImplementedException();
        }
    }
}
