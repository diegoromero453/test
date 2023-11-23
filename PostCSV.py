from flask import Flask, request, jsonify
import pyodbc
import csv
from datetime import datetime

app = Flask(__name__)

# Base de datos
server = 'localhost'
database = 'Test'
username = 'admin'
password = '28890926'
driver = 'ODBC Driver 17 for SQL Server'

# Conexión
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# Solicitud API
@app.route('/upload', methods=['POST'])
def upload():
    try:
        print("Solicitud recibida")

        # Obtener los archivos desde Postman
        hired_employees_file = request.files['hired_employees']
        departments_file = request.files['departments']
        jobs_file = request.files['jobs']

        # Verificar si se proporcionaron los archivos
        if not hired_employees_file or not departments_file or not jobs_file:
            return jsonify({'error': 'No se encuentran todos los archivos'})

        # Obtener el tamaño del lote desde los parámetros de la URL
        batch_size = request.args.get('batch_size', default=1, type=int)
        batch_size_limit = 1000
        
        if batch_size > batch_size_limit:
            print("Se limita el batch_size a 1000")
            batch_size = batch_size_limit
            
        
        # Definición de estructura de datos para los lotes de cada tabla
        batch_data_employees = []
        batch_data_departments = []
        batch_data_jobs = []
        
        row_count = 0

        # Procesar el archivo de empleados
        print("Leyendo el archivo de empleados")
        data_employees = hired_employees_file.read().decode('utf-8').splitlines()
        csv_reader_employees = csv.reader(data_employees)

        for row in csv_reader_employees:
            try:
                
                cursor.execute("SET IDENTITY_INSERT employees ON")
                if len(row) != 0:
                    hire_datetime = None
                    if row[2]:
                        hire_datetime = datetime.strptime(row[2], "%Y-%m-%dT%H:%M:%SZ")

                    batch_data_employees.append((row[0], row[1], hire_datetime, row[3], row[4]))

                    row_count += 1

                    if row_count >= batch_size:
                        cursor.executemany("""
                            INSERT INTO employees (id, name, hire_datetime, department_id, job_id)
                            VALUES (?, ?, ?, ?, ?)
                        """, batch_data_employees)

                        batch_data_employees = []
                        row_count = 0

            except Exception as e:
                print(f"Error al procesar fila de empleados {row}: {str(e)}")

        if batch_data_employees:
            cursor.executemany("""
                INSERT INTO employees (id, name, hire_datetime, department_id, job_id)
                VALUES (?, ?, ?, ?, ?)
            """, batch_data_employees)

        cnxn.commit()
        print("Carga de datos de empleados completada")

        # Procesar el archivo de departamentos
        print("Leyendo el archivo de departamentos")
        data_departments = departments_file.read().decode('utf-8').splitlines()
        csv_reader_departments = csv.reader(data_departments)

        for row in csv_reader_departments:
            try:
               
                batch_data_departments.append((row[0], row[1]))

                row_count += 1

                if row_count >= batch_size:
                    cursor.executemany("""
                        INSERT INTO departments (id, department)
                        VALUES (?, ?)
                    """, batch_data_departments)

                    batch_data_departments = []
                    row_count = 0

            except Exception as e:
                print(f"Error al procesar fila de departamentos {row}: {str(e)}")

        if batch_data_departments:
            cursor.executemany("""
                INSERT INTO departments (id, department)
                VALUES (?, ?)
            """, batch_data_departments)

        cnxn.commit()
        print("Carga de datos de departamentos completada")

        # Procesar el archivo de trabajos
        print("Leyendo el archivo de trabajos")
        data_jobs = jobs_file.read().decode('utf-8').splitlines()
        csv_reader_jobs = csv.reader(data_jobs)

        for row in csv_reader_jobs:
            try:
                
                batch_data_jobs.append((row[0], row[1]))

                row_count += 1

                if row_count >= batch_size:
                    cursor.executemany("""
                        INSERT INTO jobs (id, job)
                        VALUES (?, ?)
                    """, batch_data_jobs)

                    batch_data_jobs = []
                    row_count = 0

            except Exception as e:
                print(f"Error al procesar fila de trabajos {row}: {str(e)}")

        if batch_data_jobs:
            cursor.executemany("""
                INSERT INTO jobs (id, job)
                VALUES (?, ?)
            """, batch_data_jobs)

        cnxn.commit()
        print("Carga de datos de trabajos completada")

        return jsonify({'success': 'Files uploaded successfully'})

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)})

   

if __name__ == '__main__':
    app.run()
