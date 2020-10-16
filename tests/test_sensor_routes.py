import json
import pytest
import sqlite3
import time
import unittest

from app import app

class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
        
        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 3)

    def test_device_readings_post(self):
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rowsbefore = cur.fetchall()
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100 
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have three
        self.assertTrue(len(rows) > len(rowsbefore))

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        self.assertFalse(False)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        self.assertFalse(False)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        self.assertFalse(False)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        request = self.client().get('/devices/{}/readings/min/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select min(value) from readings where device_uuid="{}"'.format(self.device_uuid))
        row = cur.fetchall()
        #Compare API value with DB value
        self.assertTrue(json.loads(request.data)[0]['value'] == row[0][0])

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get('/devices/{}/readings/max/'.format(self.device_uuid))

         # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        #Get DB Max value
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select max(value) from readings where device_uuid="{}"'.format(self.device_uuid))
        row = cur.fetchall()

        #Compare API value with DB value
        self.assertTrue(json.loads(request.data)[0]['value'] == row[0][0])

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get('/devices/{}/readings/median/'.format(self.device_uuid))

         # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        #Get DB Max value
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('''
        SELECT AVG(value) FROM (
            SELECT value FROM readings where device_uuid="{}" ORDER BY value LIMIT 2 - (
                SELECT COUNT(*) FROM readings where device_uuid="{}"
            ) % 2 OFFSET (
                SELECT (COUNT(*) - 1) / 2  FROM readings where device_uuid="{}"
            )
        )
        '''.format(self.device_uuid, self.device_uuid, self.device_uuid))
        row = cur.fetchall()
        #Compare API value with DB value
        self.assertTrue(json.loads(request.data)[0]['value'] == row[0][0])

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get('/devices/{}/readings/mean/'.format(self.device_uuid))

         # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        #Get DB Max value
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select AVG(value) from readings where device_uuid="{}"'.format(self.device_uuid, self.device_uuid, self.device_uuid))
        row = cur.fetchall()
        #Compare API value with DB value
        self.assertTrue(json.loads(request.data)[0]['value'] == row[0][0])

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        self.assertFalse(False)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get('/devices/{}/readings/quartiles/'.format(self.device_uuid))
        
         # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        #Get DB Max value
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
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
        '''.format(
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid
        ))
        row = cur.fetchall()
        #Compare API value with DB value
        
        self.assertTrue(row[0][0] == json.loads(request.data)[0]['quartile_1'])
        self.assertTrue(row[0][1] == json.loads(request.data)[0]['quartile_3'])
        self.assertTrue(row[0][2] == json.loads(request.data)[0]['median'])

    def test_device_readings_summary(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get('/devices/{}/readings/summary/'.format(self.device_uuid))
        
         # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 1)

        #Get DB Max value
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
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
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid, 
            self.device_uuid
        ))
        row = cur.fetchall()
        print(row[0])
        #Compare API value with DB value
        self.assertTrue(row[0][0] == json.loads(request.data)[0]['quartile_1'])
        self.assertTrue(row[0][1] == json.loads(request.data)[0]['quartile_3'])
        self.assertTrue(row[0][2] == json.loads(request.data)[0]['median'])