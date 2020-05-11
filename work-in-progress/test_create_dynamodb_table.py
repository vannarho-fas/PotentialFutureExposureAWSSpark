from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb import types
from unittest.mock import patch
import random as rand

class FloatSerializer(types.TypeSerializer):
    # enables dynamo to store floats

    def _is_number(self, value):
        if isinstance(value, (int, decimal.Decimal, float)):
            return True
        return False

    # Add float-specific serialisation code
    def _serialize_n(self, value):
        if isinstance(value, float):
            with decimal.localcontext(types.DYNAMODB_CONTEXT) as context:
                context.traps[decimal.Inexact] = 0
                context.traps[decimal.Rounded] = 0
                number = str(context.create_decimal_from_float(value))
                return number

        number = super(FloatSerializer, self)._serialize_n(value)
        return number

    def _serialize_m(self, value):
        return {str(k): self.serialize(v) for k, v in value.items()}

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# TypeSerializer - patch on initialisation

with patch("boto3.dynamodb.types.TypeSerializer", new=FloatSerializer):
    db = boto3.resource('dynamodb', region_name='ap-southeast-2', endpoint_url="http://localhost:8000")
    dc = boto3.client('dynamodb', region_name='ap-southeast-2', endpoint_url="http://localhost:8000")

npv_table = db.create_table(
    TableName='pfe_sim',
    KeySchema=[
        {
            'AttributeName': 'ID',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'  # Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ID',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", npv_table.table_status)


a = rand.randrange(0, 5)
b = rand.randrange(0, 5)
ID = 1
date = "01-05-2020"
counterparty = 111
batch_num = 1
job_type = "poc"
uncoll_npv = 100.00 + a
col_npv = 100.00 + b


response = npv_table.put_item(
    Item={
        'ID': ID,
        'date': date,
        'counterparty': counterparty,
        'job_type':  job_type,
        'batch_num': batch_num,
        'row': 5,
        'uncoll_npv': uncoll_npv,
        'col_npv': col_npv
    }
)


response = npv_table.query(
    KeyConditionExpression=Key('date').eq('01-05-2020')
)

for i in response['Items']:
    print(i['date'], ":", i['uncoll_npv'])

print(dc.list_tables())
dc.delete_table(TableName='pfe_sims')
dc.get_item()


