import pymysql
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de SQLAlchemy para o ambiente de produção
ENGINE= create_engine(f"mysql+pymysql://{os.getenv('USER_DB')}:{os.getenv('PASSWORDDB')}@{os.getenv('HOST')}:{int(os.getenv('PORTA'))}/{os.getenv('DATABASE')}")
ENGINEDW = create_engine(f"mysql+pymysql://{os.getenv('USER_DB')}:{os.getenv('PASSWORDDB')}@{os.getenv('HOST')}:{int(os.getenv('PORTA'))}/{os.getenv('DATABASEDW')}")

# Configuração pymysql para o ambiente de produção
cnx = pymysql.connect(
    host=os.getenv('HOST'),
    user=os.getenv('USER_DB'),
    password=os.getenv('PASSWORDDB'),
    port=int(os.getenv('PORTA')),
    database=os.getenv('DATABASE')
)

cursor = cnx.cursor()
