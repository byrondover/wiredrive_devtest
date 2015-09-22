#!/usr/bin/env python
#
# Hadoop_Utilization.py - Query a MySQL database for Hadoop utilization metrics.
#   Exports results to a tab-separated values file.

import calendar
import datetime
import re
import subprocess
import sys
import time

def _check_requirements():
  # Don't allow the user to run a version of Python we don't support.
  version = getattr(sys, 'version_info', (0,))
  if version < (2, 3):
    raise ImportError('Logger requires Python 2.3 or later.')

def cleanup_job_name(x):
  cleanup_list = '(Avails|Ingestion|Inventory|Quality).*'
  cleaned_name = re.sub(cleanup_list, r'\1', x)
  cleaned_name = re.sub('.*(Validation)', r'%\1', cleaned_name)
  return cleaned_name

def get_cpu_usage(job_pattern, date_created):
  mysql_cmd_fmt = "SELECT IFNULL(SUM(ja.ATTR_VALUE), 0) FROM " \
    "metrics.JOB_ATTRIBUTES ja JOIN metrics.JOB j USING(JOB_ID) " \
    "WHERE j.JOB_NAME like '{0}%' AND ja.ATTR_TYPE = '{1}' AND " \
    "ja.ATTR_NAME LIKE '%CPU%' AND j.CREATED BETWEEN '{2} 00:00:00' AND " \
    "'{2} 23:59:59'"
  value_list = [job_pattern]
  for stage in ['Map', 'Reduce']:
    cmd = ['mysql', '-u', 'root', '-q', '-s', '-e',
           mysql_cmd_fmt.format(job_pattern, stage, date_created)]
    prog = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = prog.communicate()
    if prog.returncode != 0:
      print('Failed to run mysql: {0}\n{1}'.format(stdout, stderr))
      sys.exit(1)

    value_list.append(int(stdout))
  
  return (value_list[0], value_list[1], value_list[2])

def generate_cpu_histogram(date_to_check, stage, outfile):
  """
  Create a histogram for CPU usage for a given day (date_to_check) in
  minute granularity for either the 'Map' or 'Reduce' stage.
  """
  
  day_pst_tm = time.strptime('{0} 08'.format(date_to_check),
                             '%Y-%m-%d %H')
  day_pst = calendar.timegm(day_pst_tm)
  next_day = day_pst + 86400
  sql_fmt = "SELECT j.TIME_STARTED, j.TIME_FINISHED, ja.ATTR_VALUE " \
    "FROM metrics.JOB j JOIN metrics.JOB_ATTRIBUTES ja USING(JOB_ID) " \
    "WHERE ((j.TIME_STARTED BETWEEN {0} AND {1}) OR " \
    "(j.TIME_FINISHED BETWEEN {0} AND {1})) AND " \
    "j.TIME_FINISHED IS NOT NULL AND ja.ATTR_TYPE = '{2}' AND " \
    "ja.ATTR_NAME LIKE '%CPU%'"
  sql = sql_fmt.format(day_pst * 1000, next_day * 1000, stage)
  cmd = 'mysql -u root -q -s -e'.split() + [sql]
  prog = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
  stdout, stderr = prog.communicate()
  if prog.returncode != 0:
    print('Unable to run the mysql command to get histogram: {0}\n'
          '{1}'.format(stdout, stderr))
    sys.exit(1)
  
  # Create a CPU usage histogram. It is a map with every minute within the
  # specified date.
  usage_map = dict()
  for t in range(1440):
    usage_map[t] = 0.0
  
  for line in filter(lambda x: x != '', stdout.split('\n')):
    (start, finish, usage) = map(lambda x: int(x), line.split())
    t = (start / 1000 - day_pst) / 60
    usage_per_min = usage / float((finish - start) / 60000 + 1)
    while usage != 0:
      inc = min(usage_per_min, usage)
      
      # Due to the nature of the MySQL query, it is possible to end up
      # with extra minutes outside of the specified date. Prune the
      # unwanted times.
      if t in usage_map:
        usage_map[t] += inc
      
      usage -= inc
      t += 1
  
  with open(outfile, 'w') as wf:
    map(wf.write,
      ['{0}\t{1}\n'.format(x[0], x[1]) for x in usage_map.iteritems()])

def generate_job_usage(date_to_check, outfile):
  job_list_sql = "SELECT DISTINCT(JOB_NAME) FROM metrics.JOB " \
                 "WHERE CREATED BETWEEN '{0} 00:00:00' " \
                 "AND '{1} 23:59:59'".format(date_to_check, date_to_check)
  cmd = ['mysql', '-u', 'root', '-q', '-s', '-e', job_list_sql]
  job_list_prog = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
  stdout, stderr = job_list_prog.communicate()
  if job_list_prog.returncode != 0:
    print('Failed to run the mysql command: {0}\n{1}'.format(stdout,
          stderr))
    sys.exit(1)
  
  job_list = sorted(set(filter(lambda x: x != '',
                    map(cleanup_job_name, stdout.split('\n')))),
            key=lambda item: item.lower())
  
  with open(outfile, 'w') as wf:
    wf.write('Job Name\tMap CPU Time\tMap Utilization\tReduce CPU Time\t'
             'Reduce Utilization\n')
    for job in job_list:
      cpu_usage = get_cpu_usage(job, date_to_check)
      wf.write('{0}\t{1}\t{2:.2f}%\t{3}\t'
               '{4:.2f}%\n'.format(cpu_usage[0], cpu_usage[1],
                                   cpu_usage[1] * 100.0 /
                                   (2826 * 86400000.0),
                                   cpu_usage[2],
                                   cpu_usage[2] * 100.0 /
                                   (1248 * 86400000.0)))

def main():
  # Get the date from command line. Use yesterday as default.
  date_to_check = None
  if len(sys.argv) > 1:
    if not re.search('^\d{4}[/-]?\d{2}[/-]?\d{2}$', sys.argv[1]):
      print('Wrong date format.  Use YYYYMMDD, YYYY/MM/DD, or YYYY-MM-DD')
      sys.exit(1)

    date_to_check = re.sub('(\d{4}).?(\d{2}).?(\d{2})', r'\1-\2-\3',
                 sys.argv[1])
  else:
    today = datetime.date.today()
    date_to_check = '{0:4d}-{1:02d}-{2:02d}'.format(today.year, today.month,
                            today.day)
  
  map_file = 'mapper-usage-{0}.tsv'.format(date_to_check)
  generate_cpu_histogram(date_to_check, 'Map', map_file)
  reduce_file = 'reducer-usage-{0}.tsv'.format(date_to_check)
  generate_cpu_histogram(date_to_check, 'Reduce', reduce_file)

  job_file = 'job-list-{0}.tsv'.format(date_to_check)
  generate_job_usage(date_to_check, job_file)

_check_requirements()
if __name__ == '__main__':
  main()
