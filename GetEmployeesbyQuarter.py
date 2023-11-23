from flask import Flask, request, jsonify
import pyodbc
from collections import defaultdict, OrderedDict
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
@app.route('/metrics', methods=['GET'])
def get_metrics():
    try:
        # Consulta SQL para obtener el número de empleados contratados para cada trabajo y departamento en 2021 dividido por trimestre
        query = """
            SELECT
                j.job AS job,
                d.department AS department,
                COUNT(e.id) AS num_employees,
                SUM(CASE WHEN DATEPART(QUARTER, e.hire_datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
                SUM(CASE WHEN DATEPART(QUARTER, e.hire_datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
                SUM(CASE WHEN DATEPART(QUARTER, e.hire_datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
                SUM(CASE WHEN DATEPART(QUARTER, e.hire_datetime) = 4 THEN 1 ELSE 0 END) AS Q4
            FROM employees e
            JOIN jobs j ON e.job_id = j.id
            JOIN departments d ON e.department_id = d.id
            WHERE YEAR(e.hire_datetime) = 2021
            GROUP BY j.job, d.department
            ORDER BY d.department, j.job
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Estructura de datos para almacenar los resultados
        metrics_list = []
        for row in results:
            metrics_dict = OrderedDict([
                ('department', row.department),
                ('job', row.job),
                ('Q1', row.Q1),
                ('Q2', row.Q2),
                ('Q3', row.Q3),
                ('Q4', row.Q4)
            ])
            metrics_list.append(metrics_dict)

        return jsonify(metrics_list)

    except Exception as e:
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run()

