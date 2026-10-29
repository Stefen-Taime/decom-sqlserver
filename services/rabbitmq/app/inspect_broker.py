import json
from admin import api,escaped
from messaging import connection
from topology import VHOST,QUEUE,DEAD_QUEUE
with connection('/run/secrets/admin') as conn:
    channel=conn.channel()
    counts={name:channel.queue_declare(queue=name,passive=True).method.message_count for name in [QUEUE,DEAD_QUEUE]}
overview=api('GET','overview')
print(json.dumps({'version':overview['rabbitmq_version'],'queues_ready':counts,
    'policies':api('GET','policies/'+escaped(VHOST)),
    'users':[u['name'] for u in api('GET','users')],
    'permissions':api('GET','vhosts/'+escaped(VHOST)+'/permissions')}))
