import csv
from datetime import datetime
from typing import Any
import mysql.connector
import oracledb

from senhas import *


class RPA:
    def __init__(self):
        self.host = HOST
        self.query_reserva = self.get_query('reserva')
        self.query_telefones = self.get_query('telefones')
        pass

    

    def get_query(self, name):
        # ler o mysql
        if name not in ['reserva', 'telefones']:
            raise f"O nome: {name} não está disponivel"
        with open(f'querys/{name}.sql', 'r') as sql_file:
            query = sql_file.read()
        return query

    def connect_mysql(self):
        banco_mysql = mysql.connector.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )

        try:
            print(banco_mysql.is_connected())
            if banco_mysql.is_connected():
                print("Connecting to banco")
                cursor = banco_mysql.cursor()

                # Defina sua consulta SELECT
                # consulta = "SELECT cm_ip, cm_port, cm_base, cm_usuario, cm_senha FROM parametros"

                consulta = """
                                SELECT p.idcliente, p.cm_ip, p.cm_porta, p.cm_base, p.cm_usuario, p.cm_senha, c.nome AS nome_cliente
                                FROM parametro AS p
                                INNER JOIN cliente AS c ON p.idcliente = c.idcliente;

                                """

                # Execute a consulta
                cursor.execute(consulta)

                # Recupere os resultados
                resultados = cursor.fetchall()
                # print(resultados)

                return resultados

        except mysql.connector.Error as erro:
            print(f"Erro ao conectar ao MySQL: {erro}")

        # Finaliza dando certo ou não
        finally:
            cursor.close()
            banco_mysql.close()

    def save_csv(data, columns, filename):
        with open(filename, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(columns)
            # Escreva os resultados da consulta
            for row in data:
                csv_writer.writerow(row)
                print(row)

    
    def oracle_conect(self, host, username, password, port=1521):
        data = {}
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port})))"
        try:
            connection = oracledb.connect(user=username, password=password,
                                          dsn=dsn)
            print("Conexão bem-sucedida!")
            
            data['telefones'] = self.get_data(connection, self.query_telefones)
            data['reserva'] = self.get_data(connection, self.query_reserva)
           
            return data
        except Exception as e:
            print("Erro ao conectar ao Oracle:", e)

        finally:
            if 'connection' in locals():
                connection.close()
            

    def get_data(self,connection, query):
        data = {}
        with connection.cursor() as cursor:
            cursor.execute(self.query_reserva)
            data['data']: cursor.fetchall()
            data['columns']: [col[0] for col in cursor.description]
        return data
        
    
            
    def run(self):

        host = 'localhost'
        port = 1521  # Porta padrão do Oracle
        username = 'system'
        password = 'password'
        # result = self.oracle_conect(host, username, password, port)
        # self.save_csv(result,  'resultados.csv')
        # print(result)
        
        # print(query_reserva)

        clientes_hotel = self.connect_mysql()
        for cliente in clientes_hotel:
            id = cliente[0]
            ip = cliente[1]
            port = cliente[2]
            db_name = cliente[3]
            username = cliente[4]
            password = cliente[5]
            client_name = cliente[6]

            filename_reservas = f'Reservas_{id}_{client_name}_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
            filename_telefones = f'Telefones_{id}_{client_name}_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'

            result = self.oracle_conect(ip, username, password, port)
            
            columns_telefones = result['telefones']['columns']
            data_telefones =  result['telefones']['data']
            self.save_csv(data_telefones, columns_telefones, filename=filename_reservas)
            
            columns_reserva = result['reserva']['columns']
            data_reserva =  result['reserva']['data']
            self.save_csv(data_reserva, columns_reserva, filename=filename_telefones)
       
       

if __name__ == '__main__':
    rpa = RPA()
    rpa.run
    # rpa.run()#
    #print(rpa.get_query('telefones'))
    #print(rpa.host)
    # rpa.run()
