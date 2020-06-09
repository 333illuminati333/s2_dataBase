import datetime
import logging
from threading import Thread

logging.basicConfig(filename="UserEvents.log", level=logging.INFO)


class EventListener(Thread):
    def __init__(self, con):
        Thread.__init__(self)
        self.connection = con

    def run(self):
        pub_sub = self.connection.pubsub()
        pub_sub.subscribe(["users", "spam"])
        for itm in pub_sub.listen():
            if itm['type'] == 'message':
                mes = f"\n{datetime.datetime.now()} :    {itm['data']}"
                logging.info(mes)
