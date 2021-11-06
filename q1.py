"""
 Python script finding which paymentMethod had
 the highest total amount of gross revenue.
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class Q1(MRJob):
    # each input lines consists of City, Category, Revenue, PaymentMethod

    def mapper(self, _, line):
        # create a key-value pair with key: PaymentMethod, value: Revenue
        line_cols = line.split(',')
        yield line_cols[3], float(line_cols[2])

    # def combiner(self, payment_method, revenue):
    #     # input: key-value pair with key: paymentMethod, value: revenue
    #     # pulls in payment_method of the same type and sums up the revenues of all of them.
    #     yield payment_method, sum(revenue)

    def reducer(self, pay_method, revenue):
        # input: key-value pair with key: paymentMethod, value: revenue
        # pulls in payment_method of the same type and sums up the revenues of all of them.
        # output in format key: None, value: (revenue_sum, pay_method) so the lines are sorted by
        # total revenue and so that the next reducer can pull in all the outputted lines to find the maximum
        yield None, (sum(revenue), pay_method)

    def reducer_get_max(self, _, pM_rev_sum_pair):
        # input: key-value pair with key: None, value: (revenue_sum, pay_method)
        # pulls in all values and outputs only the payment method and revenue with the highest total revenue
        yield max(pM_rev_sum_pair)


    def mapper_format_number(self, total_revenue, pay_method):
        # input: key-value pair with key: paymentMethod, value: revenue, only one line should be the input
        # formats the output so that the total revenue is shown in proper comma/$ format (i.e $25,000.00 instead
        # of 25000.000000023333)
        yield pay_method, '${:,.2f}'.format(total_revenue)

    # defines the steps as the same order as the functions are defined.
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   # combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(
                   reducer=self.reducer_get_max
                   ),
            MRStep(mapper=self.mapper_format_number)
        ]


if __name__ == '__main__':
    Q1.run()
