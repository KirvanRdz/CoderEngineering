
#ESTUDIANTE: KIRVAN RODRIGUEZ
#ENTREGABLE 1

import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import requests
import os


#  API gratuita de Alpha Vantage https://www.alphavantage.co/support/#api-key
API_KEY = os.getenv("API_KEY")

# Variables base de datos
HOST=os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME= os.getenv("DBNAME")
USER = os.getenv("USER")
PASSWORD= os.getenv("PASSWORD")
# Función para obtener los datos de un stock en específico
def get_stock_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data

def extract_last_day_data(symbol):
    stock_data = get_stock_data(symbol)

    # Obtener la fecha más reciente (la clave máxima en el diccionario)
    last_date = max(stock_data['Time Series (Daily)'].keys())

    # Obtener los datos del último día
    last_day_data = stock_data['Time Series (Daily)'][last_date]

    # Eliminar el número de índice del nombre de los atributos
    cleaned_last_day_data = {key.split('. ')[1]: value for key, value in last_day_data.items()}

    # Agregar la fecha al diccionario
    cleaned_last_day_data['date'] = last_date
  
    return cleaned_last_day_data

# Función para verificar si existe la tabla en la base de datos    
def create_table_if_not_exists(cur):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS stock_data (
        symbol VARCHAR(10),
        date DATE,
        open_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        close_price FLOAT,
        volume INT,
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, date)
    );
    '''
    cur.execute(create_table_query)

# Función para insertar los datos en la base de datos
def insert_data_to_db(symbol, last_day_data):
    # Conexión a Amazon Redshift
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD
    )
    
    # Cursor para ejecutar comandos SQL
    cur = conn.cursor()
    
    # Crear la tabla si no existe
    create_table_if_not_exists(cur)

    # Verificar si el último dato ya está en la base de datos
    cur.execute("SELECT EXISTS (SELECT 1 FROM stock_data WHERE symbol = %s AND date = %s)", (symbol, last_day_data['date']))
    exists = cur.fetchone()[0]
    
    if not exists:
        # Comando SQL para insertar los datos en la tabla
        insert_query = sql.SQL('''
            INSERT INTO stock_data (symbol, date, open_price, high_price, low_price, close_price, volume, ingest_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''')
        
        # Obtener la fecha y hora actual
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Valores a insertar
        values = (symbol, last_day_data['date'], last_day_data['open'], last_day_data['high'], last_day_data['low'], last_day_data['close'], last_day_data['volume'], current_timestamp)

        # Ejecutar el comando SQL
        cur.execute(insert_query, values)
        
        # Confirmar los cambios
        conn.commit()
    # Cerrar la conexión
    cur.close()
    conn.close()


# Símbolo de Amazon
symbol = "AMZN"  

# Obtener el último dato de cotización de Amazon
last_day_data = extract_last_day_data(symbol)

# Insertar los datos en la base de datos
insert_data_to_db(symbol, last_day_data)
