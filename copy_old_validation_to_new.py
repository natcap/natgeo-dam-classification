import sqlite3

base_path = 'dam_bounding_box_db.db'
target_path = 'workspace/dam_bounding_box_db.db'

with sqlite3.connect(base_path) as conn:
    cursor = conn.cursor()
    cursor.execute('SELECT * from validation_table')
    base_validataion_entries = cursor.fetchall()

with sqlite3.connect(target_path) as conn:
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT or REPLACE INTO validation_table "
        "(bounding_box_bounds, key, metadata, username, time_date) "
        "VALUES (?, ?, ?, ?, ?)", base_validataion_entries)
