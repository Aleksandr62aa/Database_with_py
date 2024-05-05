# Working with a PostgreSQL database using Python

import hydra
import time
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection
from omegaconf.dictconfig import DictConfig


@hydra.main(version_base=None, config_path="configs", config_name="postgre_config")
def main(config: DictConfig) -> None:
    
    # ПОДКДЮЧЕНИЕ, СОЗДАНИЕ И УДАЛЕНИЕ БД
    # подключение к БД postgres
    db_conn = config['db_info']['db_connection']    
    conn_params = {
            "db_name": db_conn['db_name'],
            "db_user":  db_conn['db_user'],
            "db_password": db_conn['db_password'],
            "db_host": db_conn['db_host'],
            "db_port": db_conn['db_port'],            
        }    
    # connection = create_connection(**conn_params)
    
    # удалить БД bd_app
    drop_database_query = f"DROP DATABASE {config['db_info']['name']};"
    # execute_query(connection, drop_database_query)       
    
    # создать БД bd_app
    create_database_query = f"CREATE DATABASE {config['db_info']['name']}"
    # execute_query(connection, create_database_query)

    # подключение к БД bd_app
    conn_params = {
            "db_name":  config['db_info']['name'],
            "db_user":  db_conn['db_user'],
            "db_password": db_conn['db_password'],
            "db_host": db_conn['db_host'],
            "db_port": db_conn['db_port'],            
        }    
    connection = create_connection(**conn_params)
       
    # СОЗДАТЬ ТАБЛИЦЫ (команда CREATE TABLE)    
    # создать таблицу  users
    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL, 
    age INTEGER,
    gender TEXT,
    nationality TEXT
    )
    """   
    execute_query(connection, create_users_table)

    # создать таблицу  posts
    create_posts_table = """
    CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY, 
    title TEXT NOT NULL, 
    description TEXT NOT NULL, 
    user_id INTEGER REFERENCES users(id)
    )
    """
    execute_query(connection, create_posts_table)
    
    # создать таблицу  time
    create_time_table = """
    CREATE TABLE IF NOT EXISTS time (
    id SERIAL PRIMARY KEY,
    timestamp_date TIMESTAMP 
    )
    """  
    execute_query(connection, create_time_table)

    # ВСТАВКА ЗАПИСЕЙ (команда INSERT INTO)    
    # вставка записей в таблицу users 
    users = [
    ("James", 25, "male", "USA"),
    ("Leila", 32, "female", "France"),
    ("Brigitte", 35, "female", "England"),
    ("Mike", 40, "male", "Denmark"),
    ("Elizabeth", 21, "female", "Canada"),
    ]

    user_records = ", ".join(["%s"] * len(users))
    insert_query = (
    f"INSERT INTO users (name, age, gender, nationality) VALUES {user_records}"
    )    
    execute_query(connection, insert_query, data=users)
   
    # вставка записей в таблицу posts 
    posts = [
    ("Happy", "I am feeling very happy today", 1),
    ("Hot Weather", "The weather is very hot today", 2),
    ("Help", "I need some help with my work", 2),
    ("Great News", "I am getting married", 1),
    ("Interesting Game", "It was a fantastic game of tennis", 5),
    ("Party", "Anyone up for a late-night party today?", 3),
    ]

    post_records = ", ".join(["%s"] * len(posts))    
    insert_query = (
    f"INSERT INTO posts (title, description, user_id) VALUES {post_records}"
    ) 
    execute_query(connection, insert_query, data=posts)

    # вставка записей в таблицу time
    data = [(time.time(),), (time.time(),) ]   
    
    data_records = ", ".join(["(to_timestamp(%s))"] * len(data))    
    insert_query = (
    f"INSERT INTO time (timestamp_date) VALUES {data_records}"
    ) 
    execute_query(connection, insert_query, data=data)
    
    # ЧТЕНИЕ ЗАПИСЕЙ (команда SELECT * FROM)
    # чтение записей из таблицы users
    select_users = "SELECT * FROM users"
    users = execute_read_query(connection, select_users)
    for user in users:
        print(user)

    #  ОБНОВЛЕНИЕ ЗАПИСЕЙ (коанда UPDATE)
    # обновление записи в таблице posts, номер строки 2
    update_post_description = """
    UPDATE
        posts
    SET
        description = 'The weather has become pleasant now'
    WHERE
        id = 2
    """
    execute_query(connection,  update_post_description)

    # обновление записи в таблице users, номер строки 2     
    update_post_description = """
    UPDATE
        users
    SET
        age = 45
    WHERE
        id = 1
    """
    execute_query(connection,  update_post_description)

    # УДАЛЕНИЕ ЗАПИСЕЙ (команда DELETE)
    # удаление записи из таблицы posts, номер строки 3
    delete_comment = "DELETE FROM posts WHERE id = 5"
    execute_query(connection, delete_comment) 

# подключение БД
def create_connection(db_name: str, db_user: str, db_password: str,
                       db_host: str, db_port: str) -> connection:
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print(f"Connection to PostgreSQL {db_name}")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# выбор записей из таблицы
def execute_query(connection: connection, query: str, data: list=None) -> None:
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        if data == None:
            cursor.execute(query)
        else:
            cursor.execute(query, data)
        print(f"{query}")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

# выполнить запрс к БД
def execute_read_query(connection: connection, query: str) -> None:
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


if __name__ == "__main__":
    main()