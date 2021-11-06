"""
Python Script finding the number of distinct product categories
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class Q3(MRJob):
    # each input lines consists of City, Category, Revenue, PaymentMethod
    def mapper(self, _, line):
        # create a key-value pair with key: category, value: 1
        line_cols = line.split(',')
        yield line_cols[1], 1

    def reducer(self, category, count):
        # input are key-value pairs with key: category, value: [1]
        # output key:category, value:max(count)=1
        # output is the list of distinct product categories
        yield category, max(count)

    def mapper2(self, key, value):
        # input are key:category, value:1
        # just output the key:null and value:1 so next reducer can take all these values and sum them up
        yield None, value

    def reducer2(self, _, value):
        # input are key:null, value:1
        # reducer pulls all the null key values together and sums up the values
        # output "num_of_categories:", value: the number of categories there are.
        yield "num_of_categories:", sum(value)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                mapper=self.mapper2,
                reducer=self.reducer2
            )
        ]


if __name__ == '__main__':
    Q3.run()