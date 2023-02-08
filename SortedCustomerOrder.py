from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSortedCustomerOrder(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_order,
                   reducer=self.reducer_totals_by_customer),
            MRStep(
                mapper=self.mapper_make_amounts_key,
                reducer=self.reducer_output_results
            )
        ]

    def mapper_get_order(self, _, line):
        (customer, order, amount) = line.split(',')
        yield customer, float(amount)

    def reducer_totals_by_customer(self, customer, amount):
        yield customer, sum(amount)

    def mapper_make_amounts_key(self, customerID, orderTotal):
        yield '%04.02f'%float(orderTotal), customerID

    def reducer_output_results(self, orderTotal, customerIDs):
        for customerID in customerIDs:
            yield customerID, orderTotal


if __name__ == '__main__':
    MRSortedCustomerOrder.run()