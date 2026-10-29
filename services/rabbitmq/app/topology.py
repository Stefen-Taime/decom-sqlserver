"""Shared, operator-owned AMQP topology; runtime users cannot declare resources."""
EXCHANGE='exports.ready'
QUEUE='invoice-app.ready'
DEAD_EXCHANGE='exports.dead'
DEAD_QUEUE='invoice-app.dead'
ROUTING_KEY='invoice_details.v1'
VHOST='/exports'


def definitions(vhost=VHOST):
    return {
        'vhosts':[{'name':vhost}],
        'exchanges':[{'name':name,'vhost':vhost,'type':'direct','durable':True,
                      'auto_delete':False,'internal':False,'arguments':{}}
                     for name in [EXCHANGE,DEAD_EXCHANGE]],
        'queues':[{'name':name,'vhost':vhost,'durable':True,'auto_delete':False,
                   'arguments':{'x-queue-type':'quorum','x-quorum-initial-group-size':1,
                                **({'x-single-active-consumer':True} if name==QUEUE else {})}}
                  for name in [QUEUE,DEAD_QUEUE]],
        'bindings':[{'source':exchange,'vhost':vhost,'destination':queue,
                     'destination_type':'queue','routing_key':ROUTING_KEY,'arguments':{}}
                    for exchange,queue in [(EXCHANGE,QUEUE),(DEAD_EXCHANGE,DEAD_QUEUE)]],
        'policies':[
            {'name':'invoice-delivery','vhost':vhost,'pattern':'^invoice-app\\.ready$',
             'apply-to':'quorum_queues','priority':10,'definition':{
                 'delivery-limit':5,'dead-letter-exchange':DEAD_EXCHANGE,
                 'dead-letter-routing-key':ROUTING_KEY,'dead-letter-strategy':'at-least-once',
                 'overflow':'reject-publish','max-length':10000}},
            {'name':'invoice-dead-retention','vhost':vhost,'pattern':'^invoice-app\\.dead$',
             'apply-to':'quorum_queues','priority':10,'definition':{
                 'overflow':'reject-publish','max-length':10000,'delivery-limit':-1}}]}


def permissions(vhost=VHOST):
    return [
        {'user':'stefen','vhost':vhost,'configure':'.*','write':'.*','read':'.*'},
        {'user':'export_publisher','vhost':vhost,'configure':'^$','write':'^exports\\.ready$','read':'^$'},
        {'user':'invoice_app','vhost':vhost,'configure':'^$','write':'^$','read':'^invoice-app\\.ready$'}]
