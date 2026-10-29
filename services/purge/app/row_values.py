"""Exact, type-directed comparison of captured SQL Server row images; no float coercion."""
import base64
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import uuid
from audit_core import archive_images, image_key, lsn

TEXT_TYPES={'char','nchar','varchar','nvarchar','text','ntext'}
INTEGER_TYPES={'tinyint','smallint','int','bigint'}
DECIMAL_TYPES={'decimal','numeric','money','smallmoney'}
BINARY_TYPES={'binary','varbinary','image','geography','geometry'}
TIME_TYPES={'date','time','datetime','datetime2','smalldatetime'}
SUPPORTED=TEXT_TYPES|INTEGER_TYPES|DECIMAL_TYPES|BINARY_TYPES|TIME_TYPES|{'bit','uniqueidentifier'}
UNAVAILABLE='__debezium_unavailable_value'


def canonical(value, column):
    kind=column['type']
    if kind not in SUPPORTED: raise ValueError('Unsupported captured SQL type: '+kind)
    if value is None: return None
    if kind in TEXT_TYPES:
        if not isinstance(value,str): raise ValueError('Expected a string')
        return value
    if kind in INTEGER_TYPES:
        if type(value) is int: return str(value)
        if isinstance(value,str) and re.fullmatch(r'-?\d+',value): return str(int(value))
        raise ValueError('Invalid exact integer')
    if kind in DECIMAL_TYPES:
        if type(value) not in {str,int,Decimal}: raise ValueError('Approximate decimal representation rejected')
        try: number=Decimal(value)
        except InvalidOperation: raise ValueError('Invalid decimal') from None
        if not number.is_finite(): raise ValueError('Non-finite decimal')
        # Decimal.normalize() uses the active context and can round a 38-digit SQL decimal.
        rendered=format(number,'f')
        if '.' in rendered: rendered=rendered.rstrip('0').rstrip('.')
        if number.is_zero(): return '0'
        return rendered
    if kind=='bit':
        if type(value) is bool: return value
        if type(value) is str and value in {'0','1'}: return value=='1'
        raise ValueError('Invalid bit')
    if kind=='uniqueidentifier':
        if not isinstance(value,str): raise ValueError('Invalid UUID')
        return str(uuid.UUID(value))
    if kind in BINARY_TYPES:
        if not isinstance(value,str): raise ValueError('Expected base64 binary data')
        try: return base64.b64decode(value,validate=True).hex()
        except ValueError: raise ValueError('Invalid binary encoding') from None
    if not isinstance(value,str): raise ValueError('Temporal value must preserve string precision')
    raw=value.removesuffix('Z')
    if kind=='date':
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',raw): raise ValueError('Invalid date')
        date.fromisoformat(raw);return raw
    pattern=r'(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?'
    if kind=='time':
        match=re.fullmatch(pattern,raw);prefix=''
    else:
        if len(raw)<19 or raw[10]!='T': raise ValueError('Invalid timestamp')
        date.fromisoformat(raw[:10]);prefix=raw[:10]+'T';match=re.fullmatch(pattern,raw[11:])
    if not match: raise ValueError('Invalid time or unsupported temporal precision')
    hour,minute,second=map(int,match.group(1,2,3))
    if hour>23 or minute>59 or second>59: raise ValueError('Invalid clock time')
    fraction=(match.group(4) or '').ljust(7,'0')
    return f'{prefix}{hour:02}:{minute:02}:{second:02}.{fraction}'


def canonical_row(row, columns):
    if not isinstance(row,dict) or set(row)!={c['name'] for c in columns}:
        raise ValueError('Captured column set differs; missing is not NULL')
    return tuple((column['name'],canonical(row[column['name']],column)) for column in columns)


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def explained_placeholder(source_value,archive_value,column,operation,update_mask):
    """Diagnostic only: recognize the pinned connector rule, never equate a marker to NULL."""
    kind=column['type']
    is_lob=kind in {'text','ntext','image'} or (kind in {'varchar','nvarchar','varbinary'} and column.get('max_length')==-1)
    if not is_lob or source_value is not None or operation not in {3,4}: return False
    marker=UNAVAILABLE.encode().hex() if kind in BINARY_TYPES else UNAVAILABLE
    if archive_value!=marker: return False
    ordinal=column.get('ordinal')
    if type(ordinal) is not int or ordinal<1 or not isinstance(update_mask,str): return False
    if not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})+',update_mask): return False
    raw=bytes.fromhex(update_mask[2:])
    byte=len(raw)-1-(ordinal-1)//8
    return byte>=0 and not (raw[byte] & (1<<((ordinal-1)%8)))


class RowValues:
    def __init__(self, source, policy):
        self.columns=source['captured_columns'];self.policy=policy
        self.expected=defaultdict(Counter);self.observed=defaultdict(Counter)
        self.expected_rows={};self.observed_rows={};self.expected_masks={};self.issues=[];self.shape_cache=set()
        self.shapes=Counter();self.checked_cells=Counter()
        lower,upper=lsn(policy['snapshot_commit_lsn']),lsn(source['upper_lsn'])
        for table,rows in source['tables'].items():
            for row in rows:
                if not lower<lsn(row['commit_lsn'])<=upper: continue
                key=image_key(table,row['pk'],row['operation'],row['commit_lsn'],row['change_lsn'])
                values=canonical_row(row['row_data'],self.columns[table])
                checksum=digest(values)
                self.expected[key][checksum]+=1;self.expected_rows[key,checksum]=values
                self.expected_masks[key,checksum]=row.get('update_mask')

    def shape(self, table, row):
        names=frozenset(row) if isinstance(row,dict) else frozenset()
        identity=(table,names)
        if identity in self.shape_cache: return
        self.shape_cache.add(identity);self.shapes[table]+=1
        expected={c['name'] for c in self.columns[table]}
        if names!=expected:
            self.issues.append({'table':table,'reason':'column_set_mismatch',
                                'missing_columns':sorted(expected-names),'unexpected_columns':sorted(names-expected)})

    def snapshot_shape(self,event):
        table=event['source']['schema']+'.'+event['source']['table']
        self.shape(table,event['after'])

    def observe(self,event):
        table=event['source']['schema']+'.'+event['source']['table']
        rows=[event['before'],event['after']] if event['op']=='u' else [event['before'] if event['op']=='d' else event['after']]
        for key,row in zip(archive_images(event,self.policy),rows):
            self.shape(table,row)
            try: values=canonical_row(row,self.columns[table])
            except (ValueError,TypeError) as error:
                self.issues.append({'table':table,'reason':'invalid_value_representation','error_type':type(error).__name__})
                continue
            checksum=digest(values)
            self.observed[key][checksum]+=1;self.observed_rows[key,checksum]=values
            self.checked_cells[table]+=len(values)

    def report(self):
        tables={}
        for table,columns in self.columns.items():
            source_count=archive_count=matched=mismatched=placeholder_only=0
            placeholder_counts=Counter()
            differing=set()
            unexplained_columns=set()
            by_name={column['name']:column for column in columns}
            source_hash,archive_hash=hashlib.sha256(),hashlib.sha256()
            keys=sorted(k for k in set(self.expected)|set(self.observed) if k[0]==table)
            for key in keys:
                left,right=self.expected[key],self.observed[key]
                left_count,right_count=sum(left.values()),sum(right.values())
                equal=sum((left & right).values())
                source_count+=left_count;archive_count+=right_count;matched+=equal
                mismatched+=min(left_count,right_count)-equal
                source_hash.update(json.dumps([key,sorted(left.items())],separators=(',',':')).encode()+b'\n')
                archive_hash.update(json.dumps([key,sorted(right.items())],separators=(',',':')).encode()+b'\n')
                if left_count==right_count==1 and not equal:
                    a=dict(self.expected_rows[key,next(iter(left))]);b=dict(self.observed_rows[key,next(iter(right))])
                    different={name for name in a if a[name]!=b[name]}
                    differing.update(different)
                    mask=self.expected_masks[key,next(iter(left))]
                    explained={name for name in different if explained_placeholder(a[name],b[name],by_name[name],key[2],mask)}
                    placeholder_counts.update((name,key[2]) for name in explained)
                    unexplained_columns.update(different-explained)
                    placeholder_only+=int(different==explained)
            tables[table]={'source_images':source_count,'archive_images':archive_count,'matching_images':matched,
                'mismatched_images':mismatched,'differing_columns':sorted(differing),
                'placeholder_only_mismatched_images':placeholder_only,
                'unexplained_mismatched_images':mismatched-placeholder_only,
                'unexplained_differing_columns':sorted(unexplained_columns),
                'explained_placeholder_cells':sum(placeholder_counts.values()),
                'placeholder_differences':[{'column':name,'operation':operation,'cells':count}
                    for (name,operation),count in sorted(placeholder_counts.items())],
                'captured_columns':[c['name'] for c in columns],'compared_cells':self.checked_cells[table],
                'observed_archive_column_shapes':self.shapes[table],
                'source_values_sha256':source_hash.hexdigest(),'archive_values_sha256':archive_hash.hexdigest(),
                'status':'matched' if source_count==archive_count==matched and source_count else 'no_retained_images' if not source_count and not archive_count else 'mismatch'}
        passed=not self.issues and all(t['source_images']==t['archive_images']==t['matching_images'] for t in tables.values())
        return {'contract':'sqlserver-cdc-row-values/v1','matched':passed,'tables':tables,
                'source_images':sum(t['source_images'] for t in tables.values()),
                'archive_images':sum(t['archive_images'] for t in tables.values()),
                'matching_images':sum(t['matching_images'] for t in tables.values()),
                'placeholder_only_mismatched_images':sum(t['placeholder_only_mismatched_images'] for t in tables.values()),
                'explained_placeholder_cells':sum(t['explained_placeholder_cells'] for t in tables.values()),
                'unexplained_mismatched_images':sum(t['unexplained_mismatched_images'] for t in tables.values()),
                'connector_representation_explains_all_differences':not self.issues and all(
                    t['source_images']==t['archive_images']==t['matching_images']+t['placeholder_only_mismatched_images'] for t in tables.values()),
                'compared_cells':sum(self.checked_cells.values()),'issues':self.issues[:20],'issue_count':len(self.issues),
                'semantics':'Strict captured-value equality. NULL and unavailable markers remain different; explained placeholders are diagnostics, not successful value reconciliation or a purge approval.'}
