import redis
import atexit
import datetime
import logging

logging.basicConfig(filename="Events.log", level=logging.INFO)


def log_in(connection, username) -> int:
    user_id = connection.hget("users:", username)
    if not user_id:
        print("Error in username")
        return -1
    connection.sadd("online", username)
    logging.info(f"{datetime.datetime.now()} User: {username} : log in \n")

    return int(user_id)


def log_out(connection, u_id) -> int:
    username = connection.hmget(f"user: {u_id}", ["login"])[0]
    logging.info(f"{datetime.datetime.now()} User: {username} : log out \n")
    return connection.srem("online", connection.hmget(f"user:{u_id}", ["login"])[0])


def send_message(connection, text, s_id, u_id):
    try:
        mes_id = int(connection.incr('message:id:'))
        user_id = int(connection.hget("users:", u_id))
    except TypeError:
        print("Error in username")
        return

    pipeline = connection.pipeline(True)

    pipeline.hmset(f'message:{mes_id}', {
        'text': text,
        'id': mes_id,
        'sender_id': s_id,
        'consumer_id': user_id,
        'status': "created"
    })

    pipeline.lpush("queue:", mes_id)
    pipeline.hmset('message:%s' % mes_id, {
        'status': 'queue'
    })

    pipeline.zincrby("sent:", 1, f"user:{connection.hmget(f'user:{s_id}', ['login'])[0]}")
    pipeline.hincrby(f"user:{s_id}", 'queue', 1)
    pipeline.execute()

    return mes_id


def reg(connection, username):
    if connection.hget('users:', username):
        print(f"User {username} exists")
        return None

    user_id = connection.incr('user:id:')

    pipeline = connection.pipeline(True)

    pipeline.hset('users:', username, user_id)

    pipeline.hmset('user:%s' % user_id, {
        'login': username,
        'id': user_id,
        'queue': 0,
        'checking': 0,
        'blocked': 0,
        'sent': 0,
        'delivered': 0
    })
    pipeline.execute()
    logging.info(f"{datetime.datetime.now()} User: {username} : register \n")
    return user_id


def print_mes(connection, u_id):
    messages = connection.smembers(f"sentto:{u_id}")
    for mes_id in messages:
        mes = connection.hmget(f"message:{mes_id}", ["sender_id", "text", "status"])
        s_id = mes[0]
        print(f"From: {connection.hmget(f'user:{s_id}', ['login'])[0]} - {mes[1]}")
        if mes[2] != "delivered":
            pipeline = connection.pipeline(True)
            pipeline.hset(f"message:{mes_id}", "status", "delivered")
            pipeline.hincrby(f"user:{s_id}", "sent", -1)
            pipeline.hincrby(f"user:{s_id}", "delivered", 1)
            pipeline.execute()


def reg_menu() -> int:
    print(">>>>>>MENU<<<<<<")
    print("1 => Register")
    print("2 => Log in")
    print("3 => Close")
    return int(input("Enter your choice =>"))


def menu_for_user() -> int:
    print(">>>>>>MENU<<<<<<")
    print("1 => Log out")
    print("2 => My messages")
    print("3 => Send message")
    print("4 => Message progress")
    return int(input("Enter your choice =>"))


def sign_in_user(user_id):
    return user_id != -1


def user_menu(connection, user_id):
    while True:
        choice = menu_for_user()

        if choice == 1:
            log_out(connection, user_id)
            connection.publish('users',
                               f"User => {connection.hmget('user:%s' % user_id, ['login'])[0]} signed out")
            break
        elif choice == 2:
            print_mes(connection, user_id)

        elif choice == 3:
            try:
                message = input("Enter message -> ")
                u_id = input("Send to -> ")
                if send_message(connection, message, user_id, u_id):
                    print("processing...")
            except ValueError:
                print("Error in login")

        elif choice == 4:
            user = connection.hmget("user:%s" % user_id, ['queue', 'checking', 'blocked', 'sent', 'delivered'])
            print("Queue -> %s\nChecking -> %s\nBlocked -> %s\nSent -> %s\nDelivered -> %s" %tuple(user))
        else:
            print("Error input")


def main():
    def logout_handler():
        log_out(connection, cur_user_id)

    atexit.register(logout_handler)
    connection = redis.Redis(charset="utf-8", decode_responses=True)

    while True:
        choice = reg_menu()
        if choice == 1:
            login = input("Enter login:")
            reg(connection, login)
        elif choice == 2:
            login = input("Enter login:")
            cur_user_id = log_in(connection, login)
            if sign_in_user(cur_user_id):
                username = connection.hmget(f"user:{cur_user_id}", ["login"])[0]
                connection.publish('users', f"User => {username} signed in")
                user_menu(connection, cur_user_id)
        elif choice == 3:
            print("Closing program")
            break
        else:
            print("Error in input")


if __name__ == '__main__':
    main()