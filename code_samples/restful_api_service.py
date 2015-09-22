#!/usr/bin/env python
#
# RESTful_API_Service.py - Example RESTful API wrapper around the chown command.

import grp
import os
import pwd
import sys

from flask import Flask, request
from flask_restful import reqparse, Resource, Api

app = Flask(__name__)
api = Api(app)

class ChownDirectory(Resource):
  def __init__(self):
    self.reqparse = reqparse.RequestParser(bundle_errors=True)
    self.reqparse.add_argument('path', type=str, required=True,
                               help='No path provided.')
    self.reqparse.add_argument('owner', type=str, default='')
    self.reqparse.add_argument('group', type=str, default='')
    self.reqparse.add_argument('mode', type=str, default='')
    super(ChownDirectory, self).__init__()

  def get(self):
    status = 'ERROR'
    
    args = self.reqparse.parse_args()
    
    path = args['path']
    owner = args['owner']
    group = args['group']
    mode = args['mode']
    
    if owner + group + mode == '':
      return { 'error': ('(Please specify owner, group or mode.)  '
                         'Missing required parameter in the JSON body or the '
                         'post body or the query string') }, 400
    
    if not os.path.isdir(path):
      return { 'error': ('(Please ensure directory provided exists.)  '
                         'Validation of directory path and type failed') }, 400
    
    success = chdirperms(path, owner, group, mode)
    
    if success == True:
      status = 'OK'
      owner, group, mode = statdir(path)
    else:
      return { 'error': success }, 400

    return { 'status': status, 'path': args['path'], 'owner': owner, 'group': group,
             'mode': oct(mode)[-4:] }

def chdirperms(path, owner, group, mode):
  if not owner.isdigit():
    if owner != '':
      try:
        owner = pwd.getpwnam(owner).pw_uid
      except:
        return '(Failed to resolve owner UID.)  ' \
          + str(sys.exc_info()[0].__dict__)
    else:
      owner = -1
  
  if not group.isdigit():
    if group != '':
      try:
        group = grp.getgrnam(owner).gr_gid
      except:
        return '(Failed to resolve group GID.)  ' \
          + str(sys.exc_info()[0].__dict__)
    else:
      group = -1
  
  if owner + group != -2:
    try:
      os.chown(path, owner, group)
    except:
      return '(Failed to set directory ownership permissions.)  ' \
        + str(sys.exc_info()[0].__dict__)
  
  if mode != '':
    try:
      mode = int(mode, 8)
      os.chmod(path, mode)
    except:
      return '(Failed to set directory mode permissions.)  ' \
        + str(sys.exc_info()[0].__dict__)
  
  return True

def statdir(path):
  dir_info = os.stat(path)
  
  owner = pwd.getpwuid(dir_info.st_uid).pw_name
  group = grp.getgrgid(dir_info.st_gid).gr_name
  mode = dir_info.st_mode
  
  return owner, group, mode

api.add_resource(ChownDirectory, '/api/v1/directory/chown')

if __name__ == '__main__':
  app.debug = True
  app.run('0.0.0.0')
