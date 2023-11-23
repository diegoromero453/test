
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
        # Query para obtener la lista de departamentos que han contratado más empleados que la media en 2021
        query = """
            SELECT
                d.id,
                d.department,
                COUNT(e.id) AS hired
            FROM
                departments d
                JOIN employees e ON d.id = e.department_id
            WHERE
                YEAR(e.hire_datetime) = 2021
            GROUP BY
                d.id, d.department
            HAVING
                COUNT(e.id) > (SELECT AVG(hired) FROM (
                    SELECT
                        d.id AS id,
                        COUNT(e.id) AS hired
                    FROM
                        departments d
                        JOIN employees e ON d.id = e.department_id
                    WHERE
                        YEAR(e.hire_datetime) = 2021
                    GROUP BY
                        d.id
                ) AS Count)
            ORDER BY
                hired DESC
        """
        result = cursor.execute(query).fetchall()

        # Respuesta JSON
        metrics_list = []
        for row in result:
            metrics_dict = OrderedDict({
        'id': row.id,
        'department': row.department,
        'hired': row.hired
            })
            metrics_list.append(metrics_dict)



        return jsonify(metrics_list)

    except Exception as e:
        return jsonify({'error': str(e)})

    
if __name__ == '__main__':
    app.run()
