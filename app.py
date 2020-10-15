from flask import Flask, render_template, request, Response
from flask.json import jsonify
import json
import sqlite3
import time

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()

@app.route('/devices/<string:device_uuid>/readings/', methods = ['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
   
    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))
        
        conn.commit()

        # Return success
        return 'success', 201
    else:
        # Execute the query
        cur.execute('select * from readings where device_uuid="{}"'.format(device_uuid))
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200
        
@app.route('/devices/<string:device_uuid>/readings/min/', methods = ['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('select min(value) from readings where device_uuid="{}"'.format(device_uuid))
    rows = cur.fetchall()

    # Return the JSON
    return jsonify([dict(zip(['value'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/max/', methods = ['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('select MAX(value) from readings where device_uuid="{}"'.format(device_uuid))
    rows = cur.fetchall()

    # Return the JSON
    return jsonify([dict(zip(['value'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/median/', methods = ['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
   # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('''
    SELECT AVG(value) FROM (
        SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
            SELECT COUNT(*) FROM readings where device_uuid="{}"
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
        )
    )
    '''.format(device_uuid, device_uuid, device_uuid))
    rows = cur.fetchall()
    # Return the JSON
    return jsonify([dict(zip(['value'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/mean/', methods = ['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('select AVG(value) from readings where device_uuid="{}"'.format(device_uuid))
    rows = cur.fetchall()

    # Return the JSON
    return jsonify([dict(zip(['value'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods = ['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('''
    select * from
    (
        SELECT AVG(value) FROM readings where value < (
            SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
                SELECT COUNT(*) FROM readings where device_uuid="{}"
            ) % 2 OFFSET (
                SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
            )
        )
    ) as T1
    ,
    (
        SELECT AVG(value) FROM readings where value > (
        SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
            SELECT COUNT(*) FROM readings where device_uuid="{}"
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
        )
    )
     ) as T2
    ,
    (
        SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
            SELECT COUNT(*) FROM readings where device_uuid="{}"
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
        )
    )  as T3
    '''.format(device_uuid, device_uuid, device_uuid, device_uuid, device_uuid, device_uuid, device_uuid, device_uuid, device_uuid))
    rows = cur.fetchall()
    eljson = jsonify([dict(zip(['quartile_1', 'quartile_3', 'median'], row)) for row in rows])
    # Return the JSON
    return eljson, 200


@app.route('/devices/<string:device_uuid>/readings/summary/', methods = ['GET'])
def request_readings_summary(device_uuid):
# Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
     # Execute the query
    cur.execute('''
    select * from
    (
        SELECT AVG(value) FROM readings where value < (
            SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
                SELECT COUNT(*) FROM readings where device_uuid="{}"
            ) % 2 OFFSET (
                SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
            )
        )
    ) as T1
    ,
    (
        SELECT AVG(value) FROM readings where value > (
        SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
            SELECT COUNT(*) FROM readings where device_uuid="{}"
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
        )
    )
     ) as T2
    ,
    (
        SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
            SELECT COUNT(*) FROM readings where device_uuid="{}"
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
        )
    )  as T3
    ,
    (
        (
            SELECT device_uuid, COUNT(*), MAX(value), MIN(value) FROM readings where device_uuid="{}"
        ) as T4
    )
    '''.format(
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid, 
        device_uuid
    ))
    rows = cur.fetchall()
    eljson = jsonify([dict(zip(['quartile_1', 'quartile_3', 'median', 'device_uuid', 'count', 'max', 'min'], row)) for row in rows])
    # Return the JSON
    return eljson, 200

    
if __name__ == '__main__':
    app.run()
