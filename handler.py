import boto3
import time
import json
from urllib.request import urlopen
from datetime import datetime
from config import ALLOWED_FILE_FORMATS, BUCKET_NAME


def lambda_handler(event, context):

    transcribe = boto3.client('transcribe')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    key_list = []
    for obj in bucket.objects.all():
        key = obj.key
        print("Timezone ", obj.last_modified)
        file_format = key.split('.')[-1]
        if (key.startswith('voice_to_text') and (file_format in ALLOWED_FILE_FORMATS) and (obj.last_modified).replace(tzinfo=None) > datetime(2021, 10, 6, tzinfo=None)):
            key_list.append(key)
    for key in key_list:
        bucket_name = BUCKET_NAME
        file_name = key
        print(key_list)
        print("debug ", key.split('.')[0].split('/')[-1])
        s3_uri = create_uri(bucket_name, file_name)
        file_type = file_type = key.split('.')[-1]
        print("File Type", file_type)
        job_name = datetime.now().strftime("%m-%d-%Y_%H-%M-%S") + \
            key.split('.')[0].split('/')[-1]
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            LanguageCode='en-US',
            MediaFormat=file_type,
            Media={'MediaFileUri': s3_uri}
        )
        while True:
            status = transcribe.get_transcription_job(
                TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            print("In Progress")
            time.sleep(5)
        load_url = urlopen(status['TranscriptionJob']
                           ['Transcript']['TranscriptFileUri'])
        load_json = json.dumps(json.load(load_url))
        print("result ", load_json)
        client = boto3.client('s3')
        client.put_object(
            Bucket=bucket_name, Key='transcribeFile/{}.json'.format(job_name), Body=load_json)

    return {
        'statusCode': 200,
        'body': json.dumps('Transcription job created!')
    }


def create_uri(bucket_name, file_name):
    return "s3://"+bucket_name+"/"+file_name
