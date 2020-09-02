import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

plt.style.use('dark_background')

make_database = 'https://www.postgresqltutorial.com/load-postgresql-sample-database/'
'''
DIRECTIONS TO SET UP DATABASE
1) Follow directions in the link above to create you database (Using pgAdmin would be the easiest)
2) Install the psycopg2 library with the following command in cmd:
    pip install psycopg2
3) If above doesn't work use the provided environment.yml file to create your python environment 
    (will need me for password)
        open cmd and use the statement:
        conda env create -f environment.yml 
4) If using Pycharm set your environment to the one just created
5) Update your password in the function directly below
6) Link to docs are in the corresponding functions
'''


def make_connection():
    try:
        database = 'dvdrental'  # Update this if you named your database something different
        password = 'password'  # Update this to your password

        con = psycopg2.connect(database=database, user='postgres', password=password,
                               host='127.0.0.1', port='5432')  # Update password to the one that you set
        cursor = con.cursor()
        return cursor, con

    except:
        print('Connection to database failed')


def view_table(cur, table):
    cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                AND table_type='BASE TABLE';
                """)  # SQL Statement to get all of the table from the database
    all_tables = [table[0] for table in cur.fetchall()]  # Adding all tables into a list

    if table not in all_tables:
        print(f'{table} is not a table in your database\n')
        print(f'Please select from the following tables: {all_tables}')
    else:
        cur.execute(f'''
                    SELECT *
                    FROM {table};
                    ''')  # SQL statement to select all the data from the table
        values = cur.fetchall()  # Get all of the data from the selected table
        col = [col[0] for col in cur.description]  # Gets the column name of the database table
        data_table = pd.DataFrame(values, columns=col)  # Converts data to a dataframe (python spreadsheet)
        print('Preview of your data \n')
        print(data_table.head())  # Shows the first 5 rows of the data
        return data_table


def create_chart(dataframe):
    # You will have to update this to fit your data
    # Example is for the payments table
    plt.scatter(x=dataframe['payment_date'], y=df['amount'])
    plt.xlabel('Date')
    plt.ylabel('Amount')
    plt.show()
    plt.close()


def input_single_data(cur, table):
    # You will have to update to fit your data and table
    insert_docs = 'https://www.postgresql.org/docs/12/sql-insert.html'
    col = [col[0] for col in cur.description]
    cur.execute(f'''
                INSERT INTO {table} ({col})
                VALUES ( 'PUT', 'YOUR' ,'LIST', 'OF', 'SAMPLE', 'DATA', 'HERE' );
                ''')


def input_csv(cur, table, csv_file):
    # You will have to update to fit your data and table
    copy_docs = 'https://www.postgresql.org/docs/12/sql-copy.html'
    col = [col[0] for col in cur.description]
    cur.execute(f''' 
                COPY {table} ({col})
                FROM '{csv_file}' DELIMITER ',' CSV HEADER;
                ''')


if __name__ == '__main__':
    cur, connection = make_connection()
    df = view_table(cur, 'payment')
    create_chart(df)
    connection.close()  # Closes the connection to the database.
