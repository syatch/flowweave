
from flowweave import FlowWeaveTask, FlowWeaveResult

class Test(FlowWeaveTask):
    def __init__(self, prev_future):
        self.prev_future = prev_future
        self.return_data = None

    def run(self):
        # your own procedure
        return FlowWeaveResult.SUCCESS, self.return_data