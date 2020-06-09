import time
import random
from threading import Thread
from eventlistener import EventListener

import redis


class MessageWorker(Thread):

    def __init__(self, connection, delay):
        Thread.__init__(self)
        self.conn = connection
        self.delay = delay

    def run(self):
        while 1:
            mes = self.conn.brpop("queue:")
            if mes:
                mes_id = int(mes[1])

                self.conn.hmset(f'message:{mes_id}', {
                    'status': 'checking'
                })
                mes = self.conn.hmget(f"message:{mes_id}", ["sender_id", "consumer_id"])
                u_id = int(mes[0])
                c_id = int(mes[1])
                self.conn.hincrby(f"user:{u_id}", "queue", -1)
                self.conn.hincrby(f"user:{u_id}", "checking", 1)
                time.sleep(self.delay)

                text = self.conn.hmget(f"message:{mes_id}", ["text"])[0]
                is_spam = "spam" in text
                pipeline = self.conn.pipeline(True)
                pipeline.hincrby(f"user:{u_id}", "checking", -1)
                if is_spam:
                    print(f"message id {mes_id:d} is spam ")
                    sender = self.conn.hmget("user:%s" % u_id, ["login"])[0]
                    pipeline.zincrby("spam", 1, f"user:{sender}")
                    pipeline.hmset(f'message:{mes_id}', {
                        'status': 'blocked'
                    })
                    pipeline.hincrby(f"user:{u_id}", "blocked", 1)
                    pipeline.publish('spam', f"{sender} sent spam! ")
                else:
                    print(f"message id {mes_id:d} sent")
                    pipeline.hmset(f'message:{mes_id}', {
                        'status': 'sent'
                    })
                    pipeline.hincrby(f"user:{u_id}", "sent", 1)
                    pipeline.sadd(f"sentto:{c_id}", mes_id)
                pipeline.execute()


def main():
    handlers_count = 5

    connection = redis.Redis(charset="utf-8", decode_responses=True)
    listener = EventListener(connection)
    listener.setDaemon(True)
    listener.start()

    for itm in range(handlers_count):
        connection = redis.Redis(charset="utf-8", decode_responses=True)
        w = MessageWorker(connection, random.randint(0, 3))
        w.daemon = True
        w.start()
    while True:
        pass


if __name__ == '__main__':
    main()
