using System;
using Microsoft.Spark.Sql;
using System.Collections.Generic;
using System.Linq;
using static Microsoft.Spark.Sql.Functions;

namespace SparkFuzzySQL.Extensions
{
    public static class DataFrameExtensions
    {
        public static DataFrame FuzzyOr(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string[] conditions)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(GetOrCondition(temperatureValue, fuzzyVariable, conditions) > 0.5);
        }

        private static Column GetOrCondition(Column x, FuzzyVariable fuzzyVariable, string[] conditions) =>
            CalculateRequiredMembershipRatios(x, fuzzyVariable, conditions).Max();

        private static Column GetAndCondition(Column x, FuzzyVariable fuzzyVariable, string[] conditions) =>
            CalculateRequiredMembershipRatios(x, fuzzyVariable, conditions).Min();

        private static IList<Column> CalculateRequiredMembershipRatios(Column x, FuzzyVariable fuzzyVariable, string[] conditions)
        {
            return conditions.Select(condition => fuzzyVariable.GetTermByName(condition).CalculateMembershipRatio(x)).ToList();
        }

        private static Column Max(this IList<Column> columns)
        {
            var result = columns[0];
            for (var i = 1; i < columns.Count; i++)
            {
                result = Max(result, columns[i]);
            }

            return result;
        }

        private static Column Min(this IList<Column> columns)
        {
            var result = columns[0];
            for (var i = 1; i < columns.Count; i++)
            {
                result = Min(result, columns[i]);
            }

            return result;
        }

        private static Column Max(Column a, Column b) => a > b | b > a;

        private static Column Min(Column a, Column b) => a < b | b < a;
    }
}
