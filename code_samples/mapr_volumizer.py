#!/usr/bin/env python
#
# MapR_Volumizer.py - Populate and maintain MapR directory trees using values
#   parsed from a human-readable configuration file.

################################################################################
# User defined constants.

CONFIG_PATH = '/etc/mapr_volumizer/mapr_volumizer.conf'

################################################################################
# Script logic begins.

import logging
import logging.handlers
import os

from ConfigParser import ConfigParser
from datetime import datetime
from datetime import timedelta
from pprint import pformat
from socket import getfqdn
from subprocess import CalledProcessError
from subprocess import check_output
from sys import exit

################################################################################
# Parse configuration file.

config = ConfigParser()

# Initialize empty MapR volumes nest.
volumes = {'daily': [], 'hourly': [], 'static': []}

try:
  config.read(CONFIG_PATH)

  # General options.
  loglevel = config.get('General', 'loglevel')
  logfile = config.get('General', 'logfile')
  padding = config.getint('General', 'padding')
  pidfile = config.get('General', 'pidfile')
  user_group = config.get('General', 'user_group')
  user_volumes = config.getboolean('General', 'user_volumes')
  volume_prefix = config.get('General', 'volume_prefix')
  
  # Populate MapR volumes list with input from configuration file.
  for section in set(config.sections()) ^ set(['General']):
    # Convert list to dict for ease of reference.
    mapr_volume = {}
    
    mapr_volume['path'] = section
    mapr_volume['minreplication'] = config.get(section, 'minreplication')
    mapr_volume['mode'] = config.get(section, 'mode')
    mapr_volume['owner'] = config.get(section, 'owner')
    mapr_volume['replication'] = config.get(section, 'replication')
    mapr_volume['retention'] = config.getint(section, 'retention')
    mapr_volume['schedule'] = config.get(section, 'schedule')
    mapr_volume['source_cluster'] = config.get(section, 'source_cluster')
    mapr_volume['source_path'] = config.get(section, 'source_path')
    mapr_volume['type'] = config.get(section, 'type')
    
    if mapr_volume['type'] == 'mirror':
      mapr_volume['name'] = \
        'mirror.' + mapr_volume['source_cluster'] + section.replace('/', '.')
    else:
      if volume_prefix == '' or volume_prefix == 'None':
        mapr_volume['name'] = section.lstrip('/').replace('/', '.')
      else:
        mapr_volume['name'] = volume_prefix + section.replace('/', '.')
    
    if config.get(section, 'rotation') == 'daily':
      volumes['daily'].append(mapr_volume)
    elif config.get(section, 'rotation') == 'hourly':
      volumes['hourly'].append(mapr_volume)
    elif config.get(section, 'rotation') == 'static':
      volumes['static'].append(mapr_volume)
    else:
      raise SyntaxError('Unrecognized volume rotation period "%s" for volume '
                        '"%s".' % (config.get(section, 'rotation'), section))
except SyntaxError as error:
  print error
  print 'Failed to parse configuration file %s, exiting.' % CONFIG_PATH
  exit(1)

################################################################################
# Initialize logging library.

# Set up logger and log message handler.
logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(
            logfile, maxBytes=1000000, backupCount=4)

# Set desired output level and logging format.
numeric_loglevel = getattr(logging, loglevel.upper(), None)

if not isinstance(numeric_loglevel, int):
  raise ValueError('Invalid log level: %s' % loglevel)

logger.setLevel(numeric_loglevel)
log_format = logging.Formatter(fmt='%(asctime)-19s %(levelname)-8s %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(log_format)

# Add the log message handler to the logger.
logger.addHandler(handler)

# Initial log message.
logger.info('MapR Volumizer started.')
logger.info('Log level: %s.' % loglevel.upper())

# Append user volumes to static volumes list.
if user_volumes:
  logger.info('Retrieving list of authorized MapR users.')
  mapr_users_command = ['getent', 'group']
  try:
    logger.debug('Subprocess command: ' + ' '.join(mapr_users_command))
    mapr_users_raw = check_output(mapr_users_command)
  except CalledProcessError as error:
    clean_error = error.output.strip()

    logger.error('MapR users retrieval failed: %s.', clean_error)
    logger.debug('getent call returned a non-zero exit code: %s.',
                 error.returncode)
    logger.debug('Unsanitized: %s', error.output)
  
  for line in mapr_users_raw.splitlines():
    if line.startswith(user_group):
      for user in sorted(line.split(':')[-1].split(',')):
        mapr_volume = {}
        
        if volume_prefix == '' or volume_prefix == 'None':
          mapr_volume['name'] = 'user.' + user
        else:
          mapr_volume['name'] = volume_prefix + '.user.' + user
        
        mapr_volume['path'] = '/user/' + user
        mapr_volume['minreplication'] = '2'
        mapr_volume['mode'] = '0755'
        mapr_volume['owner'] = user
        mapr_volume['replication'] = '3'
        mapr_volume['type'] = 'standard'
        
        volumes['static'].append(mapr_volume)

logger.debug('Fully populated volumes list:\n' + pformat(volumes, indent=2))

################################################################################
# Establish runtime propriety.

# Check for a PID file, and exit if found. 
pid = str(os.getpid())

if os.path.isfile(pidfile):
  print "%s already exists, exiting." % pidfile
  logger.critical('PID file %s already exists, exiting.', pidfile)
  exit(1)
else:
  try:
    file(pidfile, 'w').write(pid)
  except:
    print 'Failed to write to PID file %s, exiting.' % pidfile
    logger.critical('Failed to write to PID file %s, exiting.', pidfile)
    exit(1)

# Query DNS for host FQDN.
hostname = getfqdn()

# Only proceed if script is running on primary CLDB node.
try:
  primary_cldb = check_output(['maprcli',
                               'node', 'cldbmaster', '-noheader'])
  primary_cldb = primary_cldb.strip().split()[-1]
  
  if hostname != primary_cldb:
    print 'This script must be run on the primary CLDB node: %s.' % primary_cldb
    logger.info(
      'This script must be run on the primary CLDB node: %s. Exiting.',
      primary_cldb)
    
    # Remove pid file and exit cleanly.
    os.unlink(pidfile)
    exit()
except CalledProcessError as error:
  clean_error = error.output.strip()
  
  logger.critical('Primary CLDB node check failed: %s.', error.returncode)
  logger.debug('Unsanitized: %s', error.output)
  
  print clean_error
  os.unlink(pidfile)
  exit(1)

################################################################################
# Function definitions.

# Check if volume already exists.
def check_volume_status(volume):
  status = 'failed'
  check_volume_status_command = [
    'maprcli',
    'volume', 'info', '-name', volume,
    '-columns', 'mounted', '-noheader'
  ]
  
  try:
    logger.debug('Subprocess command: ' + ' '.join(check_volume_status_command))
    volume_info = check_output(check_volume_status_command)
    mounted = volume_info.strip()
    
    if mounted == '0':
      status = 'exists'
    elif mounted == '1':
      status = 'mounted'
  except CalledProcessError as error:
    clean_error = error.output.strip()
    
    if 'No such volume' in clean_error:
      status = 'absent'
    else:
      status = clean_error
    
    logger.debug('Volume mount check returned a non-zero exit code: %s.',
                 error.returncode)
    logger.debug('Unsanitized: %s', error.output)
  
  logger.debug('Mount Status: ' + status)
  return status

# Create volume and parent directory.
def create_volume(volume, volume_date, path_date):
  logger.info('Creating volume: %s.', volume['name'] + volume_date)
  create_volume_command = [
    'maprcli',
    'volume', 'create', '-name', volume['name'] + volume_date, '-path',
    volume['path'] + path_date, '-minreplication', volume['minreplication'],
    '-replication', volume['replication'], '-createparent', '1'
  ]
  
  if volume['type'] == 'mirror':
    lookup_volume_command = [
      'maprcli',
      'volume', 'list', '-cluster', volume['source_cluster'], '-columns', 'n',
      '-filter', '[p==' + volume['source_path'] + path_date + ']', '-noheader'
    ]
    
    try:
      logger.debug('Subprocess command: ' + ' '.join(lookup_volume_command))
      source_volume = check_output(lookup_volume_command).strip()
      
      if source_volume == '':
        raise CalledProcessError(1, lookup_volume_command, 'No output')
    except CalledProcessError as error:
      clean_error = error.output.strip()
      
      logger.error('Remote volume lookup failed: %s.', clean_error)
      logger.debug('Volume list call returned a non-zero exit code: %s.',
                 error.returncode)
      logger.debug('Unsanitized: %s', error.output)
      return 1
    
    create_volume_command += [
      '-type', '1', '-source', source_volume + '@' + volume['source_cluster']
    ]
    
    if volume['schedule'] != 'none':
      create_volume_command += [
        '-schedule', volume['schedule']
      ]
  else:
    create_volume_command += [
      '-rootdirperms', volume['mode']
    ]
  
  try:
    logger.debug('Subprocess command: ' + ' '.join(create_volume_command))
    check_output(create_volume_command)
  except CalledProcessError as error:
    clean_error = error.output.strip()
    
    logger.error('Volume creation failed: %s', clean_error)
    logger.debug('Volume create call returned a non-zero exit code: %s',
                 error.returncode)
    logger.debug('Unsanitized: %s', error.output)
    return 1
  
  if volume['type'] == 'standard':
    set_permissions(volume, path_date)

# Generate iterable arrays of volume and path formatted date strings.
def generate_date_strings(from_date, to_date, scratch=False):
  # Initialize empty datetime string lists.
  daily_volume_dates = []
  daily_path_dates = []
  hourly_volume_dates = []
  hourly_path_dates = []
  
  if scratch == True:
    # Initialize datetime string lists with static scratch directories.
    daily_volume_dates.append('.scratch')
    daily_path_dates.append('/scratch')
    hourly_volume_dates.append('.scratch')
    hourly_path_dates.append('/scratch')
  
  for delta in range(from_date, to_date):
    daily_volume_dates.append('.' + datetime.strftime(datetime.now() +
                              timedelta(days=delta), "%Y.%m.%d"))
    daily_path_dates.append('/' + datetime.strftime(datetime.now() +
                            timedelta(days=delta), "%Y/%m/%d"))
    
    for hour in range(0, 24):
      hour = str(hour).zfill(2)
      hourly_volume_dates.append('.' + datetime.strftime(datetime.now() +
                                 timedelta(days=delta), "%Y.%m.%d") + '.' +
                                 hour)
      hourly_path_dates.append('/' + datetime.strftime(datetime.now() +
                               timedelta(days=delta), "%Y/%m/%d") + '/' + hour)
  
  return daily_volume_dates, daily_path_dates, \
         hourly_volume_dates, hourly_path_dates

# Mount volume to appropriate path.
def mount_volume(volume, volume_date, path_date):
  logger.info('Mounting volume: %s.', volume['name'] + volume_date)
  mount_volume_command = [
     'maprcli',
     'volume', 'mount', '-name', volume['name'] + volume_date,
     '-path', volume['path'] + path_date
  ]
  try:
    logger.debug('Subprocess command: ' + ' '.join(mount_volume_command))
    check_output(mount_volume_command)
  except CalledProcessError as error:
    clean_error = error.output.strip()
    
    logger.error('Volume mount failed: %s.', clean_error)
    logger.debug('Volume mount check returned a non-zero exit code: %s.',
                 error.returncode)
    logger.debug('Unsanitized: %s', error.output)
  
  if volume['type'] == 'standard':
    set_permissions(volume, path_date)

# Remove daily and hourly volumes within a given date range.
def remove_volumes(volume, volume_dates):
  for volume_date in volume_dates:
    fqvn = volume['name'] + volume_date
    
    # Ascertain volume status.
    volume_status = check_volume_status(fqvn)
    
    # Do what needs to be done.
    if volume_status == 'absent':
      logger.info('Verified volume %s does not exist.', fqvn)
    elif volume_status == 'exists' or volume_status == 'mounted':
      logger.info('Removing volume: %s.', fqvn)
      remove_volume_command = [
        'maprcli',
        'volume', 'remove', '-name', fqvn, '-force', '1'
      ]
      try:
        logger.debug('Subprocess command: ' + ' '.join(remove_volume_command))
        check_output(remove_volume_command)
      except CalledProcessError as error:
        clean_error = error.output.strip()
      
        logger.error('Volume removal failed: %s.', clean_error)
        logger.debug('Volume remove call returned a non-zero exit code: %s.',
                     error.returncode)
        logger.debug('Unsanitized: %s', error.output)
    elif volume_status == 'failed':
      logger.error(
        'An unspecified error occurred while checking the status of volume %s.',
        fqvn)
    else:
      logger.error('Error: %s.', volume_status)

# Recursively set volume directory permissions.
def set_permissions(volume, path_date):
  logger.info('Setting volume owner and group permissions to: %s.',
              volume['owner'] + ':' + volume['owner'])
  set_permissions_command = [
    'hadoop',
    'fs', '-chown', volume['owner'] + ':' + volume['owner'],
    volume['path'] + path_date
  ]
  try:
    logger.debug('Subprocess command: ' + ' '.join(set_permissions_command))
    check_output(set_permissions_command)
  except CalledProcessError as error:
    clean_error = error.output.strip()
    
    logger.error('Setting volume permissions failed: %s.', clean_error)
    logger.debug('Volume permissions call returned a non-zero exit code: %s.',
                 error.returncode)
    logger.debug('Unsanitized: %s', error.output)

# Create and mount volumes through external calls to maprcli.
def volumize(volumes, volume_date = '', path_date = ''):
  for volume in volumes:
    # Ascertain volume status.
    volume_status = check_volume_status(volume['name'] + volume_date)
    
    # Do what needs to be done.
    if volume_status == 'absent':
      create_volume(volume, volume_date, path_date)
    elif volume_status == 'exists':
      mount_volume(volume, volume_date, path_date)
    elif volume_status == 'mounted':
      logger.info('Verified volume %s exists and is mounted.',
                  volume['name'] + volume_date)
    elif volume_status == 'failed':
      logger.error(
        'An unspecified error occurred while checking the status of volume %s.',
        volume['name'] + volume_date)
    else:
      logger.error('Error: %s.', volume_status)

################################################################################
# Create, mount and clean-up managed MapR volumes.

# Generated iterable array of dates for volume creation.
daily_volume_dates, daily_path_dates, hourly_volume_dates, hourly_path_dates = \
  generate_date_strings(-1, padding + 1, scratch=True)

logger.debug('Daily volume dates over which to iterate:\n' + 
             pformat(daily_volume_dates, indent=2))
logger.debug('Daily path dates over which to iterate:\n' +
             pformat(daily_path_dates, indent=2))
logger.debug('Hourly volume dates over which to iterate:\n' +
             pformat(hourly_volume_dates, indent=2))
logger.debug('Hourly path dates over which to iterate:\n' +
             pformat(hourly_path_dates, indent=2))

# Create static volumes.
volumize(volumes['static'])

# Create hourly volumes.
for volume_date, path_date in zip(hourly_volume_dates, hourly_path_dates):
  volumize(volumes['hourly'], volume_date, path_date)

# Create daily volumes.
for volume_date, path_date in zip(daily_volume_dates, daily_path_dates):
  volumize(volumes['daily'], volume_date, path_date)

# Spring cleaning.
for volume in volumes['hourly']:
  if volume['retention'] > -1:
    remove_hourly_volume_dates = generate_date_strings(
      -volume['retention'] - padding - 1, -volume['retention'])[2]
    remove_volumes(volume, remove_hourly_volume_dates)

for volume in volumes['daily']:
  if volume['retention'] > -1:
    remove_daily_volume_dates = generate_date_strings(
      -volume['retention'] - padding - 1, -volume['retention'])[0]
    remove_volumes(volume, remove_daily_volume_dates)

################################################################################
# Au revoir, Shosanna.

# Remove pid file and exit cleanly.
logger.info('Volumizing complete.')
os.unlink(pidfile)
exit()
