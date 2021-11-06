"""
 Python script finding the top three cities that had the most
 total amount of gross revenue.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class Q2(MRJob):
    # each input lines consists of City, Category, Revenue, PaymentMethod

    def mapper(self, _, line):
        # create a key-value pair with key: City, value: Revenue
        line_cols = line.split(',')
        yield line_cols[0], float(line_cols[2])

    def reducer_init(self):
        self.count = 0

    def reducer(self, city, revenue):
        # input: key-value pair with key: city, value: revenue
        # pulls in city of the same type and sums up the revenues of all of them.
        # output in format key: None, value: (revenue_sum, city) so the lines are sorted by
        # total revenue and so that the next reducer can pull in all the outputted lines to find the maximum
        yield None, (sum(revenue), city)

    def reducer_get_max(self, _, pM_rev_sum_pair):
        # input: key-value pair with key: None, value: (revenue_sum, pay_method)
        # pulls in all values and outputs only the payment method and revenue with the highest total revenue
        yield max(pM_rev_sum_pair)


    def mapper_format_number(self, total_revenue, city):
        # input: key-value pair with key: total_revenue, value: city, only one line should be the input
        # formats the output so that the total revenue is shown in proper comma/$ format (i.e $25,000.00 instead
        # of 25000.000000023333)
        yield city, '${:,.2f}'.format(total_revenue)

    # defines the steps as the same order as the functions are defined.
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer),
            MRStep(
                   reducer=self.reducer_get_max
                   ),
            MRStep(mapper=self.mapper_format_number)
        ]


if __name__ == '__main__':
    Q2.run()
