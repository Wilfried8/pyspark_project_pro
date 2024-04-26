import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = 'pyspark application'

current_location = os.getcwd()

source_olap = current_location + '/source/olap'
source_oltp = current_location + '/source/oltp'

