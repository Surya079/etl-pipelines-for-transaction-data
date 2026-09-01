import unittest
import random
import datetime
from api.utills import get_random_date_utc


class TestGenerators(unittest.TestCase):

    def test_random_transactions_time(self):

        now = datetime.datetime.now(datetime.timezone.utc)
        random_seconds = random.randint(0, 30 * 24 * 60 * 60)
        trans_dt_utc1 = now - datetime.timedelta(seconds=random_seconds)
        trans_dt_utc2 = get_random_date_utc()

        self.assertNotEqual(trans_dt_utc1, trans_dt_utc2)
