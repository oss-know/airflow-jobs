import pandas as pd


class TimeSeries:
    """Simple wrapper for a time series.

    The raw data are stored in a plain array, augmented with information
    about start end end date of the series. It's not sufficient to take
    the first end final list element for this purpose because there may
    be data points outside the intended range."""

    def __init__(self):
        self.series = []
        self.start_index = -1  # index of the first commit of the initial development window
        self.start_date = ""
        self.end_index = -1  # index of the last commit
        self.end_date = ""
        self.window_size = 1
        self.step = 1
        self.window_num = 1

    def set_start(self, start_index, start_date):
        self.start_index = start_index
        self.start_date = start_date

    def get_start(self):
        if (self.start_index == -1):
            raise Exception("Time series start date is undefined")
        return self.start_index, self.start_date

    def set_end(self, end_index, end_date):
        self.end_index = end_index
        self.end_date = end_date

    def get_end(self):
        if (self.end_index == -1):
            raise Exception("Time series end date is undefined")
        return self.end_index, self.end_date

    def set_series(self, series):
        self.series = series

    def set_slide_window(self, window_size, step, window_num):
        self.window_size = window_size
        self.step = step
        self.window_num = window_num

    def generate_window(self):
        for window_start in range(self.start_index, self.end_index, self.step):
            window_end = min(window_start + self.window_size, self.end_index + 1)
            print(window_start, '\n', window_end)
            yield self.series[window_start:window_end]

    def find_ealiest_window(self):
        cur_index = 0
        start_time = pd.to_datetime(self.series[cur_index]['committed_date'])
        end_time = min(pd.to_datetime(self.end_date), start_time + pd.DateOffset(years=2))
        cur_time = pd.to_datetime(self.series[cur_index]['committed_date'])

        while start_time <= end_time:
            committer = set()
            month_window_end_time = start_time + pd.DateOffset(months=1)
            self.start_index = cur_index
            while cur_time >= start_time and cur_time <= month_window_end_time and cur_index <= self.end_index:
                committer.add(self.series[cur_index]['committer_name'])
                cur_index += 1
                cur_time = pd.to_datetime(self.series[cur_index]['committed_date'])
            if len(committer) >= 3:
                # print(committer)
                # print(self.start_index)
                self.start_date = self.series[self.start_index]['committed_date']
                return
            start_time = month_window_end_time
        self.start_index = -1
        raise Exception("No months window meets requirements")
