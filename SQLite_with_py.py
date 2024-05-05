# Working with a SQLite database using Python

import sqlite3
from sqlite3 import Error, Connection



def main() -> None:
    
    # ПОДКДЮЧЕНИЕ, СОЗДАНИЕ БД
    # создать и подключиться к БД 
    connection = create_connection("d:\\bd_app.db3")

    # СОЗДАТЬ ТАБЛИЦЫ (команда CREATE TABLE)    
    # создать таблицу users
    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER,
    gender TEXT,
    nationality TEXT
    );
    """
    execute_query(connection, create_users_table) 

    # создать таблицу posts
    create_posts_table = """
    CREATE TABLE IF NOT EXISTS posts(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    title TEXT NOT NULL, 
    description TEXT NOT NULL, 
    user_id INTEGER NOT NULL, 
    FOREIGN KEY (user_id) REFERENCES users (id)
    );
    """
    execute_query(connection, create_posts_table)

    # создать таблицу comments
    create_comments_table = """
    CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    text TEXT NOT NULL, 
    user_id INTEGER NOT NULL, 
    post_id INTEGER NOT NULL, 
    FOREIGN KEY (user_id) REFERENCES users (id) FOREIGN KEY (post_id) REFERENCES posts (id)
    );
    """
    execute_query(connection, create_comments_table)
    
    # создать таблицу likes    
    create_likes_table = """
    CREATE TABLE IF NOT EXISTS likes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    user_id INTEGER NOT NULL, 
    post_id integer NOT NULL, 
    FOREIGN KEY (user_id) REFERENCES users (id) FOREIGN KEY (post_id) REFERENCES posts (id)
    );
    """
    execute_query(connection, create_likes_table)  # создать таблицу  traffic_info
    
    # создать таблицу time  
    create_time_table = """
    CREATE TABLE IF NOT EXISTS time (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date text 
    )
    """  
    execute_query(connection, create_time_table)

    # ВСТАВКА ЗАПИСЕЙ (команда INSERT INTO)
    # вставка записей в таблицу users
    insert_users = """
    INSERT INTO
        users (name, age, gender, nationality)
    VALUES
        ('James', 25, 'male', 'USA'),
        ('Leila', 32, 'female', 'France'),
        ('Brigitte', 35, 'female', 'England'),
        ('Mike', 40, 'male', 'Denmark'),
        ('Elizabeth', 21, 'female', 'Canada');
    """
    execute_query(connection, insert_users) 

    # вставка записей в таблицу posts
    insert_posts = """
    INSERT INTO
        posts (title, description, user_id)
    VALUES
        ("Happy", "I am feeling very happy today", 1),
        ("Hot Weather", "The weather is very hot today", 2),
        ("Help", "I need some help with my work", 2),
        ("Great News", "I am getting married", 1),
        ("Interesting Game", "It was a fantastic game of tennis", 5),
        ("Party", "Anyone up for a late-night party today?", 3);
        """
    execute_query(connection, insert_posts)

    # вставка записей в таблицу comments
    insert_comments = """
    INSERT INTO
        comments (text, user_id, post_id)
    VALUES
        ('Count me in', 1, 6),
        ('What sort of help?', 5, 3),
        ('Congrats buddy', 2, 4),
        ('I was rooting for Nadal though', 4, 5),
        ('Help with your thesis?', 2, 3),
        ('Many congratulations', 5, 4);
    """
    execute_query(connection, insert_comments)
    
    # вставка записей в таблицу posts
    insert_likes = """
    INSERT INTO
    likes (user_id, post_id)
    VALUES
    (1, 6),
    (2, 3),
    (1, 5),
    (5, 4),
    (2, 4),
    (4, 2),
    (3, 6);
    """
    execute_query(connection, insert_likes)

    # вставка записей в таблицу time
    insert_time = """
    INSERT INTO
        time (date)
    VALUES
        (datetime('now'))        
    """
    execute_query(connection, insert_time)

    # ЧТЕНИЕ ЗАПИСЕЙ (команда SELECT * FROM)    
    # чтение записей из таблицы users
    select_users = "SELECT * FROM users"
    users = execute_read_query(connection, select_users)
    for user in users:
        print(user)

    # чтение записей из таблиц users и posts операция типа JOIN
    select_users_posts = """
    SELECT
        users.id,
        users.name,
    posts.description
    FROM
        posts
    INNER JOIN users ON users.id = posts.user_id
    """
    users_posts = execute_read_query(connection, select_users_posts)
    for users_post in users_posts:
        print(users_post)
    
    # чтение записей из таблиц post и likes команда WHERE
    select_post_likes = """
    SELECT
        description as Post,
        COUNT(likes.id) as Likes
    FROM
        likes,
        posts
    WHERE
    posts.id = likes.post_id
    GROUP BY
        likes.post_id
    """
    post_likes = execute_read_query(connection, select_post_likes)
    for post_like in post_likes:
        print(post_like)

    #  ОБНОВЛЕНИЕ ЗАПИСЕЙ (команда UPDATE)
    # обновление записи в таблице posts, номер строки 2
    update_post_description = """
    UPDATE
        posts
    SET
        description = "The weather has become pleasant now"
    WHERE
        id = 2
    """
    execute_query(connection, update_post_description)
    
    # УДАЛЕНИЕ ЗАПИСЕЙ (команда DELETE)
    # удаление записи из таблицы comments, номер строки 5
    delete_comment = "DELETE FROM comments WHERE id = 5"
    execute_query(connection, delete_comment)    

def create_connection(path: str) -> Connection:
    connection = None
    try:
        connection = sqlite3.connect(path)
        print(f"Connection to SQLite DB {path}")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection: Connection , query: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print(f"Query executed {query}")
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection: Connection , query: str)  -> None:
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


if __name__ == "__main__":
    main()