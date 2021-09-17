import time
import json
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

#dynamodb functions
def get_users_query(uid: 'User ID', dynamodb: 'Boto3 dynamodb connection') -> "results from uid's last query":
    table = dynamodb.Table('capstoneBot')
    response = table.query(
        KeyConditionExpression=Key('userId').eq(uid)
    )
    return response['Items'][0]
        
def remove_result(row: 'dictionary/single row for user', dynamodb: 'Boto3 dynamodb connection'):
    table = dynamodb.Table('capstoneBot')
    response = table.update_item(
        Key={
            'userId': row['userId']
        },
        UpdateExpression="set queryId=:q, results=:r",
        ExpressionAttributeValues={
            ':q': row['queryId'],
            ':r': row['results'][1:]
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

# kendra feedback function
def submit_feedback(row: 'dictionary/single row for user', event: 'lex event object'):
    kendra = boto3.client('kendra')
    
    index_id = 'f07f485e-c67c-43b5-8cd0-83d878435d5a'
    query_id = row['queryId']
    result_id = row['results'][0]['Id']
    feedback_item = {'ClickTime': int(time.time()),
        'ResultId':result_id}
    
    if event['inputTranscript'] in ['y', 'yes']:
        relevance_value = 'RELEVANT'
    else:
        relevance_value = 'NOT_RELEVANT'
        
    relevance_item = {'RelevanceValue': relevance_value,
        'ResultId':result_id
        }
    
    response=kendra.submit_feedback(
        QueryId = query_id,
        IndexId = index_id,
        ClickFeedbackItems = [feedback_item],
        RelevanceFeedbackItems = [relevance_item]
    )


# create message to send to user
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

def lambda_handler(event: 'lex event object', context):
    try:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
        row = get_users_query(event['userId'], dynamodb)
        submit_feedback(row, event)
        if event['inputTranscript'] in ['y', 'yes']:
            return build_responce('Thank you, Your feedback as been submited! :smiling_face_with_3_hearts:')
        else:
            if len(row['results']) > 1:
                remove_result(row, dynamodb)
                message = get_s3_message(row['results'][1]['DocumentId'])
                return build_responce(message)
            else:
                return build_responce('Thank you, Your feedback as been submited! :face_with_monocle: Try repharsing your question.')
    except:
        return build_responce('I ran into some issue. :white_frowning_face: Please, try searching again. If the issue persist, try back later.')
