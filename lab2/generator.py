import random
from threading import Thread
import user
from faker import Faker
import redis
import atexit


class User(Thread):
    def __init__(self, connection, username, list_users, users_count):
        Thread.__init__(self)
        self.connection = connection
        self.users_list = list_users
        self.count_users = users_count
        user.reg(connection, username)
        self.user_id = user.log_in(connection, username)

    def run(self):
        for itm in range(10):
            mes = faker.sentence(nb_words=5, variable_nb_words=True, ext_word_list=None) \
                if random.choice([True, False]) else 'spam'
            rec = users[random.randint(0, count_users - 1)]
            print(f"mes: {mes} -> {rec}")
            user.send_message(self.connection, mes, self.user_id, rec)


def handler_closing():
    redis_conn = redis.Redis(charset="utf-8", decode_responses=True)
    online = redis_conn.smembers("online")
    for itm in online:
        redis_conn.srem("online", itm)
    print("Closing")


if __name__ == '__main__':
    count_users = 7
    flows = []
    atexit.register(handler_closing)
    faker = Faker()
    users = [faker.profile(fields=['username'], sex=None)['username'] for user in range(count_users)]
    for itm in range(count_users):
        connection = redis.Redis(charset="utf-8", decode_responses=True)
        print(f"User -> {users[itm]}")
        flows.append(User(redis.Redis(charset="utf-8", decode_responses=True), users[itm], users, count_users))
    for flow in flows:
        flow.start()

