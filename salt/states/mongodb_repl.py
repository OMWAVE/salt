# -*- coding: utf-8 -*-
'''
Management of Mongodb replica sets.
'''

import logging

log = logging.getLogger(__name__)

def initialized(name,
                user=None,
                password=None,
                host=None,
                port=None,
                members=[]):
    '''
    Ensure that the replica set is properly initialized.

    name
        The id of the replica set to initialize

    user
        The user to connect as

    password
        The password of the user

    host
        The host to connect to

    port
        The port to connect to

    members
        Members to include in the replica set
    '''
    ret = {'name': name,
           'changes': {},
           'result': True,
           'comment': ''}

    status = __salt__['mongodb.status'](user, password, host, port)
    if 'repl' in status: # Replica set already initialized
        repl_status = __salt__['mongodb.rs_status'](user, password, host, port)
        need_reconfigure = repl_status['set'] != name
        need_reconfigure |= len(repl_status['members']) != len(members)
        for existing in repl_status['members']:
            ok = False
            for m in members:
                if existing['_id'] == m['_id']:
                    ok = True
                    break

            if not ok:
                need_reconfigure |= True
                break
        if need_reconfigure:
            if __opts__['test']:
                ret['result'] = None
                ret['comment'] = ('Replica set {0} is not initialized and needs to be initialized'
                                  ).format(name)
                return ret
            reconfigure = __salt__['mongodb.rs_reconfigure'](name, user, password, host, port, members=members, version=status['repl']['setVersion'] + 1)
            if reconfigure == True:
                ret['comment'] = 'Replica set {0} has been reconfigured'.format(name)
                ret['changes'][name] = 'Reconfigured'
                return ret
            else:
                ret['comment'] = reconfigure
                ret['result'] = False
                return ret

        else:
            ret['comment'] = 'Replica set {0} is already initialized'.format(name)
            return ret

    else:
        if __opts__['test']:
            ret['result'] = None
            ret['comment'] = ('Replica set {0} is not initialized and needs to be initialized'
                              ).format(name)
            return ret

        res = __salt__['mongodb.rs_init'](name, user, password, host, port, members=members)
        ret['comment'] = 'Replica set {0} has been initialized'.format(name)
        ret['changes'][name] = 'Initialized'
        return ret