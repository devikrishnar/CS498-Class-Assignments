#import json

#def lambda_handler(event, context):
    # TODO implement
#    return {
#        'statusCode': 200,
#        'body': json.dumps('Hello from Lambda!')
#    }

import json
import boto3
from collections import defaultdict

def bfs_shortest_path(graph, start, goal):
    # keep track of explored nodes
    explored = []
    # keep track of all the paths to be checked
    queue = [[start]]
 
    # return path if start is goal
    if start == goal:
        return 0
 
    # keeps looping until all possible paths have been checked
    while queue:
        # pop the first path from the queue
        path = queue.pop(0)
        # get the last node from the path
        node = path[-1]
        if node not in explored:
            neighbours = graph[node]
            # go through all neighbour nodes, construct a new path and
            # push it into the queue
            for neighbour in neighbours:
                new_path = list(path)
                new_path.append(neighbour)
                queue.append(new_path)
                # return path if neighbour is goal
                if neighbour == goal:
                    return len(new_path) - 1
 
            # mark node as explored
            explored.append(node)
 
    # in case there's no path between the 2 nodes
    return -1


def lambda_handler1(event, context):
    print('Received event: ' +
        json.dumps(event, indent=2))
    # Instanciating connection objects with DynamoDB using boto3 dependency
    dynamodb = boto3.resource('dynamodb')
    #client = boto3.client('dynamodb')
    
    # Getting the table the table Temperatures object
    tableShortestDistance = dynamodb.Table('Shortest_Distance')
    
    # Getting the current datetime and transforming it to string in the format bellow
    
    graph = event['graph']
    d = defaultdict(list)
    s = set()

    for x in graph.split(","):
        city = x.split("->")
        d[city[0]].append(city[1])
        d[city[1]].append(city[0])
        s.add(city[0])
        s.add(city[1])

    l = list(s)
    
    # Putting a try/catch to log to user when some error occurs
    try:

        for i in range(0, len(l)):
            for j in range(0, len(l)):
                dist = bfs_shortest_path(d, l[i], l[j])
                tableShortestDistance.put_item(
                Item={
                        'Source': l[i],
                        'Destination': l[j],
                        'Distance': dist
                    }
                )
        print ("Written values into table")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Success!')
        }
    except:
        print('Closing lambda function')
        return {
                'statusCode': 400,
                'body': json.dumps('Error saving the distance')
        }
        
        
def get_distance(cityA, cityB):
    print("Inside get distance")    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Shortest_Distance')

    try:
        response = table.get_item(Key={'Source': cityA, 'Destination': cityB})
        print(response)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        if "Item" in response:
            return int(response['Item']['Distance'])
        else:
            return int(-1)
        

def lambda_handler2(event, context):
    print('received request: ' + str(event))
    cityA = event['currentIntent']['slots']['CityA']
    cityB = event['currentIntent']['slots']['CityB']
    
    short_dist = get_distance(cityA, cityB)
    
    
    response = {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
              "contentType": "SSML",
              "content": "{sd}".format(sd=short_dist)
            },
        }
    }
    print('result = ' + str(response))
    return response
    
def lambda_handler(event, context):
    print('received request: ' + str(event))
    if "graph" in event:
        return lambda_handler1(event, context)
    else:
        return lambda_handler2(event, context)