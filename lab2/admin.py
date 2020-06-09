import redis


def main():
    brake_loop = False
    connection = redis.Redis(charset="utf-8", decode_responses=True)

    while not brake_loop:
        my_choice = menu_for_admin()

        if my_choice == 1:
            online_users = connection.smembers("online")
            print("Online ->")
            for user in online_users:
                print(user)
            print("<------->")
        elif my_choice == 2:
            senders = connection.zrange("sent:", 0, 4, desc=True, withscores=True)
            print("Top 5 senders")
            for index, sender in enumerate(senders):
                print(sender[0], ": ", int(sender[1]), "mes")

        elif my_choice == 3:
            spam_users = connection.zrange("spam", 0, 4, desc=True, withscores=True)
            print("Top 5 spam users")
            for index, spam_user in enumerate(spam_users):
                print(spam_user[0], ": ", int(spam_user[1]), " spam")

        elif my_choice == 4:
            print("Closing program")
            brake_loop = True
        else:
            print(">>>>>>Error in input<<<<<<")


def menu_for_admin():
    print(">>>>>>Menu for admins<<<<<<")
    print("1 => Online")
    print("2 => Top 5 senders")
    print("3 => Top 5 spam users")
    print("4 => Close program")
    return int(input("Enter your choice -> "))


if __name__ == '__main__':
    main()
