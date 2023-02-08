from mrjob.job import MRJob


class MRCustomerOrder(MRJob):

    def mapper(self, _, line):
        (customer, order, amount) = line.split(',')
        yield customer, float(amount)

    def reducer(self, customer, amount):
        yield customer, sum(amount)


if __name__ == '__main__':
    MRCustomerOrder.run()