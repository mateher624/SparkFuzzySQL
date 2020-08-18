using System;
using Microsoft.Spark.Sql;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Spark.Sql.Catalog;

namespace SparkFuzzySQL.Extensions
{
    public static class DataFrameExtensions
    {
        public static DataFrame FuzzyOr(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string[] conditions)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(GetOrCondition(temperatureValue, fuzzyVariable, conditions) >= 0.5);
        }

        public static DataFrame FuzzyAnd(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string[] conditions)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(GetAndCondition(temperatureValue, fuzzyVariable, conditions) >= 0.5);
        }

        public static RelationalGroupedDataset FuzzyGroupBy(this DataFrame dataFrame, FuzzyVariable fuzzyVariable)
        {
            var frame = dataFrame.Projection(fuzzyVariable, false);
            return frame.GroupBy(frame["_condition"]);
        }

        public static DataFrame FuzzyJoin(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, DataFrame right)
        {
            var left = dataFrame.Projection(fuzzyVariable);
            right = right.Projection(fuzzyVariable);

            return left.Join(right, left[fuzzyVariable.GetName()] == right[fuzzyVariable.GetName()]);
        }

        public static DataFrame Projection(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, bool changeColumn = true)
        {
            var originalFrame = dataFrame;
            var newFrame = default(DataFrame);
            foreach (var term in fuzzyVariable.GetTerms())
            {
                var frame = originalFrame.WithColumn("_condition", Functions.When(ProcessCondition(dataFrame[fuzzyVariable.GetName()], fuzzyVariable, term.GetName()) >= 0.5, term.GetName()).Otherwise(""));
                newFrame = newFrame != null ? newFrame.Union(frame) : frame;
            }

            if (changeColumn)
            {
                var columns = newFrame.Columns().ToList();
                columns.Remove(fuzzyVariable.GetName());
                newFrame = newFrame.Select(columns.Select(c => newFrame[c]).ToArray());
            }

            newFrame = newFrame.Filter(newFrame["_condition"] != "");

            if (changeColumn)
            {
                newFrame = newFrame.WithColumnRenamed("_condition", fuzzyVariable.GetName());
            }

            return newFrame;

        }

        private static Column GetOrCondition(Column x, FuzzyVariable fuzzyVariable, string[] conditions) =>
            CalculateRequiredMembershipRatios(x, fuzzyVariable, conditions).Max();

        private static Column GetAndCondition(Column x, FuzzyVariable fuzzyVariable, string[] conditions) =>
            CalculateRequiredMembershipRatios(x, fuzzyVariable, conditions).Min();

        private static IList<Column> CalculateRequiredMembershipRatios(Column x, FuzzyVariable fuzzyVariable, string[] conditions)
        {
            return conditions.Select(condition => ProcessCondition(x, fuzzyVariable, condition)).ToList();
        }

        private static Column ProcessCondition(Column x, FuzzyVariable fuzzyVariable, string condition)
        {
            return fuzzyVariable.GetTermByName(condition).CalculateMembershipRatio(x);
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

        private static Column Max(Column a, Column b) => Functions.When(a > b, a).Otherwise(b);

        private static Column Min(Column a, Column b) => Functions.When(a < b, a).Otherwise(b);
    }
}
