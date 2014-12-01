import math

class BulkInserter:
    batch_size = 1000
    
    def __init__(self, amount_, space_, generator_):
        self.amount = amount_
        self.batch = 0
        self.batch_count = math.ceil(self.amount / self.batch_size)
        self.space = space_
        self.generator = generator_

    def run(self):
        while self.batch < self.batch_count:
            self.next_batch()
            self.batch = self.batch + 1
        
    def next_batch(self, ):
        pending_ops = []
        batch_amount = self.batch_size
        
        # Last batch might not be full size
        if self.batch+1 == self.batch_count:
            batch_amount =  self.amount - (self.batch_size * self.batch)
    
        for i in range(batch_amount):
                pos = self.batch * self.batch_size + 1
                p = self.space.async_insert(self.generator(pos))
                pending_ops.append(p)

        while len(pending_ops) > 0:
                pending_ops[0].wait()
                pending_ops.pop(0)
