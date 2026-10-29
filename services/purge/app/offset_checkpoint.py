"""Read a bounded Kafka FileOffsetBackingStore without Java deserialization.

This accepts only the HashMap<byte[], byte[]> wire format. It never loads classes
or executes serialized code. A new format or partition requires explicit review.
"""
import json
import math
import struct
from audit_core import lsn

MAX_FILE=1024*1024
HASHMAP=bytes.fromhex('00116a6176612e7574696c2e486173684d61700507dac1c31660d103000246000a6c6f6164466163746f724900097468726573686f6c647870')
BYTE_ARRAY=bytes.fromhex('00025b42acf317f8060854e00200007870')


class Reader:
    def __init__(self,data):
        if not isinstance(data,bytes) or not 0<len(data)<=MAX_FILE: raise ValueError('Offset file size invalid')
        self.data=data;self.pos=0;self.handles=[]

    def take(self,count):
        if count<0 or self.pos+count>len(self.data): raise ValueError('Truncated offset file')
        result=self.data[self.pos:self.pos+count];self.pos+=count;return result

    def expect(self,value):
        if self.take(len(value))!=value: raise ValueError('Unsupported offset serialization')

    def integer(self): return struct.unpack('>i',self.take(4))[0]

    def reference(self):
        index=self.integer()-0x7e0000
        if not 0<=index<len(self.handles): raise ValueError('Invalid serialized reference')
        return self.handles[index]

    def array(self):
        token=self.take(1)
        if token==b'\x71':
            value=self.reference()
            if not isinstance(value,bytes): raise ValueError('Reference is not a byte array')
            return value
        if token!=b'\x75': raise ValueError('Offset entries must be byte arrays')
        token=self.take(1)
        if token==b'\x72':
            self.expect(BYTE_ARRAY);self.handles.append('byte-array-class')
        elif token==b'\x71':
            if self.reference()!='byte-array-class': raise ValueError('Wrong array class reference')
        else: raise ValueError('Unsupported array descriptor')
        size=self.integer()
        if not 0<size<=65536: raise ValueError('Offset byte array size invalid')
        value=self.take(size);self.handles.append(value);return value

    def read(self):
        self.expect(b'\xac\xed\x00\x05\x73\x72');self.expect(HASHMAP)
        self.handles.extend(['hash-map-class','hash-map'])
        factor=struct.unpack('>f',self.take(4))[0];threshold=self.integer()
        if not math.isfinite(factor) or not 0<factor<=1 or not 0<=threshold<=1048576:
            raise ValueError('Unsupported HashMap parameters')
        self.expect(b'\x77\x08');capacity=self.integer();count=self.integer()
        if not 1<=capacity<=1048576 or not 1<=count<=32: raise ValueError('Offset map bounds invalid')
        entries=[(self.array(),self.array()) for _ in range(count)]
        self.expect(b'\x78')
        if self.pos!=len(self.data): raise ValueError('Trailing serialized content')
        if len({key for key,value in entries})!=len(entries): raise ValueError('Duplicate offset key')
        return entries


def unique_object(pairs):
    result={}
    for key,value in pairs:
        if key in result: raise ValueError('Duplicate JSON member in offset')
        result[key]=value
    return result


def decode(data,database='WideWorldImporters',server='wwi'):
    entries=Reader(data).read()
    if len(entries)!=1: raise ValueError('Only the single configured SQL Server partition is supported')
    key,value=[json.loads(raw.decode('utf-8'),object_pairs_hook=unique_object,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Non-finite JSON'))) for raw in entries[0]]
    if (not isinstance(key,list) or len(key)!=2 or not isinstance(key[0],str) or not key[0]
            or key[1]!={'server':server,'database':database}):
        raise ValueError('Offset namespace or source partition differs')
    if not isinstance(value,dict): raise ValueError('Offset value is not an object')
    if set(value)-{'commit_lsn','change_lsn','event_serial_no','command_id','snapshot','snapshot_completed'}:
        raise ValueError('Unreviewed offset fields or incremental/transaction state')
    if value.get('snapshot') not in (None,False,'false') or value.get('snapshot_completed') not in (None,True):
        raise ValueError('Offset is not a completed streaming checkpoint')
    serial=value.get('event_serial_no');command=value.get('command_id')
    if type(serial) is not int or serial<1: raise ValueError('Missing offset event serial number')
    if command is not None and (type(command) is not int or command<0): raise ValueError('Invalid command id')
    return {'namespace':key[0],'partition':key[1],'commit_lsn':lsn(value.get('commit_lsn')),
            'change_lsn':lsn(value.get('change_lsn')),'event_serial_no':serial,'command_id':command}


def position(value):
    # Pinned TxLogPosition orders direct-mode command_id before the intra-TX LSN.
    # Callers must reject comparisons between modes. -1 represents function mode.
    command=value.get('command_id');serial=value['event_serial_no']
    if command is not None and (type(command) is not int or command<0): raise ValueError('Invalid command id')
    if type(serial) is not int or serial<1: raise ValueError('Invalid event serial number')
    return (lsn(value['commit_lsn']),-1 if command is None else command,lsn(value['change_lsn']),serial)


def archive_coordinate(value):
    """Archive lookup key only; never a complete direct-mode resume ordering."""
    return (lsn(value['commit_lsn']),lsn(value['change_lsn']),value['event_serial_no'])
