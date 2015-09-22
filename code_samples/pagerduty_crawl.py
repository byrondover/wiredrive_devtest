#!/usr/bin/env python
#
# PagerDuty_Crawl.py - Crawls PagerDuty API for alerts, and displays them in
#   both parsable and human-readable formats. Written for backwards
#   compatibility with Python 2.4, hence optparse.

################################################################################
# User defined constants.

LOGFILE_PATH = '/tmp/pd_crawl.log'     # Location to dump temporary log files.
PAGERDUTY_URL = 'https://pagerduty.com'  # Register at PagerDuty.com.
API_TOKEN = '<API_TOKEN_GOES_HERE>'     # Generate online, /api_keys.

# PagerDuty services to query.
SERVICES = [
  'PIYAW84', # 911 Email Ladder
  'P52N86V'  # Pingdom Service
]

################################################################################
# Script begins.

import sys
import optparse
import requests           # Requires Requests (python-requests.org).
from   datetime import datetime
from   datetime import timedelta

# Parse input arguments and flags.
parser = optparse.OptionParser()
parser.add_option("-d", "--date",
                  dest = "arg_date",
                  help = "Format: MM/DD/YYYY. "
                  "Specify the 'since' date from which to run pull log data.")
parser.add_option("-D", "--reporter-script-date",
                  dest = "arg_rdate",
                  help = "Format: Mon Jun 17 17:32:37 PDT 2013. "
                  "Typically inserted during opsview_reporter script runtime.")
parser.add_option("-H", "--human-readable",
                  action = "store_true", dest = "flag_human", default = False,
                  help = "Print log messages in a readable format to stdout.")

(options, args) = parser.parse_args()

# Establish current time zone difference.
timedifference = datetime.utcnow() - datetime.now()

# Truncate log file.
if options.flag_human != True:
  print 'pagerduty.dump\t',
  
  logfile = open(LOGFILE_PATH + '/' + 'pagerduty.dump', 'w')
  logfile.truncate()

# Check and set dates.
if options.arg_date != None:
  # Check user date fidelity.
  try:
    tmp_date = datetime.strptime(options.arg_date, "%m/%d/%Y")
  except:
    print "Date was not understood. Required format: MM/DD/YYYY."
    sys.exit()
  
  since = datetime.strftime(tmp_date + timedifference, "%Y-%m-%dT%H:%M")
elif options.arg_rdate != None:
  # Check reporter script date fidelity.
  try:
    tmp_rdate = datetime.strptime(options.arg_rdate, "%a %b %d %H:%M:%S %Z %Y")
  except:
    print "Date input not understood."
    print "Required format: Mon Jun 17 17:32:37 PDT 2013."
    sys.exit()
  
  since = datetime.strftime(tmp_rdate + timedifference, "%Y-%m-%dT%H:%M")
else:
  # If no date specified, pull incidents from last 24 hours.
  since = datetime.strftime(datetime.now() + timedifference - timedelta(days=1),
                            "%Y-%m-%dT%H:%M")

# Script will always crawl for incident logs from 'since' date to current date.
until = datetime.strftime(datetime.now() + timedifference, "%Y-%m-%dT%H:%M")

# Fetch incident list from PagerDuty API.
for service in SERVICES:
  url = PAGERDUTY_URL + '/api/v1/incidents'
  headers = { 'Authorization': 'Token token=' + API_TOKEN }
  payload = {
    'since': since,
    'until': until,
    'service': service,
    'fields': 'id,incident_key,incident_number,service'
  }
  
  r = requests.get(url, headers=headers, params=payload)
  
  # Trim the fat.
  incidents_dict = r.json()
  incidents = incidents_dict['incidents']
  
  # Create blank log lines list for later use.
  log_lines = []
  
  # Iterate through incidents, pull, parse and store pertinent log entries.
  for incident in incidents:
    url = PAGERDUTY_URL + '/api/v1/incidents/' + incident['id'] + '/log_entries'
    headers = { 'Authorization': 'Token token=' + API_TOKEN }
    payload = {
      'since': since,
      'until': until
    }
    
    r = requests.get(url, headers=headers, params=payload)
  
    # Trim the fat.
    entries_dict = r.json()
    entries = entries_dict['log_entries']
    
    for entry in entries:
      # Ensure we parse only log entries we care about.
      if not entry['type'] in ('assign', 'acknowledge'):
        continue
      
      # Subtract time zone difference from UTC datetime returned by PagerDuty.
      entry_date = datetime.strptime(entry['created_at'], "%Y-%m-%dT%H:%M:%SZ") - timedifference
      
      # Determine alert status severity and assign team member.
      if entry['type'] in ('acknowledge'):
        status = 'OK'
        team_member = entry['agent']['name']
      else:
        status = 'CRITICAL'
        team_member = entry['assigned_user']['name']
      
      # Combine issue ID and description, pulled from email subject, for later use.
      description = entry['type'].upper() + ": " + str(incident['incident_number']) + " (" + incident['incident_key'] + ")"
      
      # Consolidate information into separate log lines.
      if options.flag_human == True:
        # Populate values list with data pulled from PagerDuty.
        values = [
          # Format datetime string in accordance with opsview_reporter script output convention.
          entry_date.strftime("%Y/%m/%d %H:%M"),
          incident['service']['name'],      # PagerDuty service.
          team_member,              # Name of on-call team member.
          status,                 # OK if resolve or acknowledge, otherwise CRITICAL.
          description               # Description of issue, parsed.
        ]
        
        # Append each log line with tab separated values.
        log_lines.append('\t '.join(map(str,values)))
      else:
        # Populate values list with data pulled from PagerDuty.
        values = [
          # Format datetime string in accordance with Nagios log convention.
          '[' + entry_date.strftime('%s') + '] SERVICE NOTIFICATION: oncall ' + incident['service']['name'],
          incident['service']['name'],      # PagerDuty service.
          team_member,              # Name of on-call team member.
          status,                 # OK if resolve or acknowledge, otherwise CRITICAL.
          description               # Description of issue, parsed.
        ]
        
        # Append each log line with semicolon separated values.
        log_lines.append('; '.join(map(str,values)))

  # Output complete list of log lines.
  if options.flag_human == True:
    # Print all log lines.
    for line in sorted(log_lines):
      print line
  else:
    # Store log lines to parsable file.
    for line in sorted(log_lines):
      logfile.write(line)
      logfile.write("\n")
      print '.',

if options.flag_human != True:
  logfile.close()

print '\tDone.'

# End of file.