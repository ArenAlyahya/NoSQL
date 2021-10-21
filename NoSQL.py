import boto3
import csv

from boto3.dynamodb.conditions import Key, Attr


#In order for the code to run successfully it is better to enter the credential info as in the following link in the terminal
#https://sysadmins.co.za/interfacing-amazon-dynamodb-with-python-using-boto3/
#$ aws configure
#AWS Access Key ID [****************XYZ]: 
#AWS Secret Access Key [****************xyz]: 
#Default region name [eu-west-1]: 
#Default output format [json]: 

def create_bucket():
	s3 = boto3.resource('s3',
	aws_access_key_id = '',
	aws_secret_access_key ='')
	s3.create_bucket(Bucket = 'aren-alyahya-bucket')
	#s3.Object('aren-alyahya-bucket','experiments.csv').put( Body = open('experiments.csv','rb'))
	# , CreateBucketConfiguration = {'LocationConstraint':'us-east-1'}




def create_DataTable(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name ='us-east-1')

    table = dynamodb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH'  # Partition key
            },

            {
                'AttributeName': 'Temp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Id',
                'AttributeType': 'S'  # Partition key
            },
           
            {
                'AttributeName': 'Temp',
                'AttributeType': 'S'
            }

        ],

        #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.01.html
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName = 'DataTable')
    return table


def read_csv_file():

    s3 = boto3.resource('s3',
    aws_access_key_id = 'AKIAZXEWTCLXZK5AEJUY',
    aws_secret_access_key ='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')
   
    dynamodb = boto3.resource('dynamodb', region_name ='us-east-1')
    table = dynamodb.Table("DataTable")

    urlbase = "https://s3.us-east-1.amazonaws.com/aren-alyahya-bucket/"
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html 

    with open('experiments.csv','r') as csvfile:
        csvf = csv.reader(csvfile, delimiter =',', quotechar ='|')
        
        # This skips the first row of the CSV file. https://evanhahn.com/python-skip-header-csv-reader/
        next(csvf)

        for item in csvf:
            body = open(item[4] , 'rb')
            s3.Object('aren-alyahya-bucket', item[4]).put(Body=body)
            md = s3.Object('aren-alyahya-bucket',item[4]).Acl().put(ACL = 'public-read')
            url = urlbase + item[4]
            metedata_item = {'Id' : item[0], 'Temp' : item[1], 
            'Conductivity': item [4],'Concentration': item[2], 'url':url}

            table.put_item(Item = metedata_item)

def query():
    # https://sysadmins.co.za/interfacing-amazon-dynamodb-with-python-using-boto3/
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('DataTable')

    response = table.query(
        KeyConditionExpression=Key('Id').eq('1') & Key('Temp').eq('-1')
        )

    items = response['Items']
    print(items)





if __name__ == '__main__':

	print("\nCreating aren-alyahya-bucket\n")
	create_bucket()
	print("ren-alyahya-bucket is successfully created ^^\n")

	print("\nCreate DynamoDB Table (DataTable)..\n")
	table = create_DataTable()
	print("DataTable is successfully created ^^\n")

	print("\nRead the metadata from a CSV file, and upload the data objects to aren-alyahya-bucket..\n")
	read_csv_file()
	print("Metadata  is read successfully , and opjects are uploded successfully ^^\n")

	print("\nExecute a query where Id = 1 and Temp = -1 :\n")
	query()



    #print("Table status:", movie_table.table_status)