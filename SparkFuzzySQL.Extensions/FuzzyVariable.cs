using System.Collections.Generic;
using System.Linq;

namespace SparkFuzzySQL.Extensions
{
    public class FuzzyVariable
    {
        private readonly string _name;
        private readonly IList<FuzzyTerm> _terms;

        public FuzzyVariable(string name)
        {
            _name = name;
            _terms = new List<FuzzyTerm>();
        }

        public string GetName() => _name;

        public void AddTerm(FuzzyTerm term) => _terms.Add(term);

        public FuzzyTerm GetTermByName(string name) => _terms.FirstOrDefault(t => t.GetName() == name);

    }
}
