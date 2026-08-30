import datetime
import random


def get_random_date_utc():
    now = datetime.datetime.now(datetime.timezone.utc)
    random_seconds = random.randint(0, 30 * 24 * 60 * 60)

    return now - datetime.timedelta(seconds=random_seconds)



