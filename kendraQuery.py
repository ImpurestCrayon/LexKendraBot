import json
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

# Dynamodb Class
class DynamodbTable:
    def __init__(
            self, 
            table_name: 'name of dynamodb table'='capstoneBot',
            endpoint_url: 'dynamodb url endpoint'="https://dynamodb.us-east-1.amazonaws.com"
            ) -> None:
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)
        self.dynamodb_table = self.dynamodb.Table(table_name)
        
    def update_table(self, row: 'dictionary with userId, queryId, and results'):
        if self.is_new_user(row['userId']):
            self.new_row(row)
        else:
            self.update_row(row)
        
    def is_new_user(self, uid: 'User ID') -> 'Boolean for if user is new or not':
        response = self.dynamodb_table.query(
            KeyConditionExpression=Key('userId').eq(uid)
        )
        return len(response['Items']) < 1
        
    def new_row(self, row: 'dictionary with userId, queryId, and results'):
        response = self.dynamodb_table.put_item(
           Item=row
        )
        
    def update_row(self, row: 'dictionary with userId, queryId, and results'):
        response = self.dynamodb_table.update_item(
            Key={
                'userId': row['userId']
            },
            UpdateExpression="set queryId=:q, results=:r",
            ExpressionAttributeValues={
                ':q': row['queryId'],
                ':r': row['results']
            },
            ReturnValues="UPDATED_NEW"
        )

# Kendra Function
def kendra_query(event: 'lex event object') -> 'S3 document ID:
    client = boto3.client('kendra')
    indexId = 'f07f485e-c67c-43b5-8cd0-83d878435d5a'
    userInput = event['inputTranscript']
    query = client.query(
        IndexId=indexId,
        QueryText=userInput
    )
    if len(query['ResultItems']) < 1:
        return "Sorry, I could not find any websites related to your question."
    else:
        docId = query['ResultItems'][0]['DocumentId']
    row = dict(
        userId = event['userId'],
        queryId = query['QueryId'],
        results = query['ResultItems']
    )
    dyno_table = DynamodbTable()
    dyno_table.update_table(row)
    return docId

# create message to send
def get_s3_message(docId: str) -> 'string message to be displayed to user':
    s3 = boto3.client('s3')
    docId = docId.replace('s3://', '')
    bucket = docId[:docId.find('/')]
    key = docId[docId.find('/')+1:]
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    name = json_content['name']
    url = json_content['url']
    description = json_content['description']
    message = f'Here is a link to {name} ({url}). Description: {description} :simple_smile: Was this what you were looking for?'
    return message

# Lex Respone
def build_responce(message: str) -> 'lex message object':
    return {
        "dialogAction" : {
            "type" : "Close",
            "fulfillmentState" : "Fulfilled",
            "message" : {
                "contentType" : "PlainText",
                "content" : message
            }
        }
    }

def lambda_handler(event, context):
    result = kendra_query(event)
    if result != "Sorry, I could not find any websites related to your question.":
        result = get_s3_message(result)
    return build_responce(result)
