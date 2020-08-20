using System;
using Microsoft.Spark.Sql;

namespace SparkFuzzySQL.Extensions.MembershipFunction
{
    public class TriangularMembershipFunction : IMembershipFunction
    {
        public double A { get; set; }
        public double B { get; set; }
        public double C { get; set; }

        public TriangularMembershipFunction(double a, double b, double c)
        {
            if (!(a <= b && b <= c))
            {
                throw new ArgumentException("Incorrect relationship between the triangle coordinates");
            }

            A = a;
            B = b;
            C = c;
        }

        public Column GetValueFor(Column x)
        {
            return Functions.When(x.Lt(A), 0.0)
                .When(x.Lt(B), ((x - A) / (B - A)))
                .When(x.Lt(C), ((-x + C) / (C - B)))
                .Otherwise(0.0);
        }
    }
}
