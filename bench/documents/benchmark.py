import time

class Benchmark:
        def __init__(self):
                self.startTime = -1

        def start(self):
                self.startTime = time.time()

        def stop(self):
                if self.startTime < 0:
                        raise RuntimeError('You didn not call start yet')

                elapsed = time.time() - self.startTime                
                self.startTime = -1
                return elapsed
