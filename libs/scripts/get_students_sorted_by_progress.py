import sys
import datetime
import pandas as pd
import re
import csv
from urllib.parse import unquote
from database_services import *


""" 
* Fetches users sorted by average progress througout each course.
"""
def get_students_sorted_by_total_progress(connection):
    users_average_total_progress_query = ''' 
    select unique_users.user_name AS unique_user_name, unique_users.user_id AS unique_user_id FROM (
        select userAndIDs.user_name as user_name, uniqueUserIds.user_id as user_id from (
            select 
                log_line #>> '{context, user_id}' AS user_id 
            from logs 
            GROUP BY user_id 
            ) uniqueUserIds
        LEFT JOIN (
            select 
                log_line -> 'username' as user_name,
                log_line #>> '{context, user_id}' AS user_id 
            from logs 
            where log_line -> 'username' != 'null' and log_line -> 'username' != '""' and log_line -> 'username' is not null
            GROUP BY user_id, user_name
        ) userAndIDs
        ON uniqueUserIds.user_id = userAndIDs.user_id
    ) unique_users 
    INNER JOIN (
        SELECT 
            log_line #>> '{context, user_id}' AS user_id,
            (log_line ->> 'context')::json ->> 'course_id' AS course_id,
            (log_line ->> 'context')::json ->> 'percent' AS course_progress_percentage
        FROM logs
        WHERE
            log_line ->> 'event_type' LIKE '%grade_calculated' 
    ) course_process_data
    ON unique_users.user_id = course_process_data.user_id
    '''
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(users_average_total_progress_query)
    average_progress = cursor.fetchall()
    cursor.close()
    connection.commit()
    return average_progress

def write_result_to_file(result_file, result):
    print('Start writing the data to file.')
    with open(result_file, mode='w', encoding='utf-8') as res_file:
        field_names = ['course_id', 'average time to enroll']
        result_file_writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        result_file_writer.writerow(field_names)
        for res in result:
            result_file_writer.writerow(res)

def main(argv):
    print('Start calculating average progress for each student.')
    
    database_name = argv[1]
    user_name = argv[2]
    result_file = argv[3]

    connection = open_db_connection(database_name, user_name)
    enrollment_distribution = get_students_sorted_by_total_progress(connection)
    close_db_connection(connection)

    write_result_to_file(result_file, enrollment_distribution)
    print(f'The result file could be found at ${result_file}')


if __name__ == '__main__':
    main(sys.argv)

