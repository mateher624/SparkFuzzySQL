using Microsoft.Spark.Sql;
using System.Collections.Generic;
using System.Linq;

namespace SparkFuzzySQL.Extensions
{
    public static class DataFrameExtensions
    {
        private const double DefaultThreshold = 0.5;
        private const string TempColumnName = "_condition";

        public static DataFrame FuzzyOr(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string[] conditions, double threshold = DefaultThreshold)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(GetOrCondition(temperatureValue, fuzzyVariable, conditions) >= threshold);
        }

        public static DataFrame FuzzyAnd(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string[] conditions, double threshold = DefaultThreshold)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(GetAndCondition(temperatureValue, fuzzyVariable, conditions) >= threshold);
        }

        public static DataFrame FuzzySelect(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, string condition, double threshold = DefaultThreshold)
        {
            var temperatureValue = dataFrame[fuzzyVariable.GetName()];
            return dataFrame.Filter(ProcessCondition(temperatureValue, fuzzyVariable, condition) >= threshold);
        }

        public static DataFrame Projection(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, bool changeColumn = true, double threshold = DefaultThreshold)
        {
            var originalFrame = dataFrame;
            var newFrame = default(DataFrame);
            foreach (var term in fuzzyVariable.GetTerms())
            {
                var frame = originalFrame.WithColumn(TempColumnName, Functions.When(ProcessCondition(dataFrame[fuzzyVariable.GetName()], fuzzyVariable, term.GetName()) >= threshold, term.GetName()).Otherwise(""));
                newFrame = newFrame != null ? newFrame.Union(frame) : frame;
            }

            if (changeColumn)
            {
                var columns = newFrame?.Columns().ToList();
                if (columns != null)
                {
                    columns.Remove(fuzzyVariable.GetName());
                    newFrame = newFrame.Select(columns.Select(c => newFrame[c]).ToArray());
                }
            }

            newFrame = newFrame?.Filter(newFrame[TempColumnName] != "");

            if (changeColumn)
            {
                newFrame = newFrame?.WithColumnRenamed(TempColumnName, fuzzyVariable.GetName());
            }

            return newFrame;
        }

        public static RelationalGroupedDataset FuzzyGroupBy(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, double threshold = DefaultThreshold)
        {
            var frame = dataFrame.Projection(fuzzyVariable, false, threshold);
            return frame.GroupBy(frame[TempColumnName]);
        }

        public static DataFrame FuzzyJoin(this DataFrame dataFrame, FuzzyVariable fuzzyVariable, DataFrame right, double threshold = DefaultThreshold)
        {
            var left = dataFrame.Projection(fuzzyVariable, threshold: threshold);
            right = right.Projection(fuzzyVariable, threshold: threshold);

            return left.Join(right, left[fuzzyVariable.GetName()] == right[fuzzyVariable.GetName()]);
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
