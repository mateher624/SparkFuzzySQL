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

        public Column GetValueFor(Column x)
        {
            return Functions.When(x.Lt(A), 0.0)
                .When(x.Lt(B), ((x - A) / (B - A)))
                .When(x.Lt(C), 1.0)
                .When(x.Lt(D), ((-x + D) / (D - C)))
                .Otherwise(0.0);
        }
    }
}
