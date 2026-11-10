import json
import os
import uuid
import boto3
import base64
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')

TABLE_NAME = os.environ['ORDENES_TABLE']
TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
VALIDAR_TOKEN_FUNC = os.environ['USUARIOS_VALIDAR_LAMBDA']

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
}

def validar_token(headers):
    """
    Extrae el token y llama a la Lambda de A (fsm-ms-usuarios).
    Espera respuesta con statusCode 200 y body con tenant_id.
    """
    headers_lower = {k.lower(): v for k, v in headers.items() if headers}
    auth_header = headers_lower.get('authorization', '')
    
    if not auth_header.startswith('Bearer '):
        raise ValueError("Falta el token o formato inválido")
        
    token = auth_header.split(' ')[1]
    
    try:
        # Contrato exacto con auth.py de Persona A
        response = lambda_client.invoke(
            FunctionName=VALIDAR_TOKEN_FUNC,
            InvocationType='RequestResponse',
            Payload=json.dumps({"token": token})
        )
        payload = json.loads(response['Payload'].read())
        
        if payload.get('statusCode') != 200:
            raise ValueError("Token inválido o expirado")
            
        # El body viene como string jsonífico o dict dependiendo de la implementación HTTP de A
        body = payload.get('body', {})
        if isinstance(body, str):
            body = json.loads(body)
            
        return body.get('tenant_id', 'tenant-1')
        
    except Exception as e:
        print(f"Error al validar token con ms-usuarios: {e}")
        raise ValueError("No autorizado")

def crear_orden(event, context):
    try:
        # 1. Validación delegada a MS-A
        tenant_id = validar_token(event.get('headers', {}))
        
        # 2. Parseo y validación de campos obligatorios para MS-D (Visitas)
        body = json.loads(event.get('body', '{}'))
        
        cliente_id = body.get('cliente_id') 
        sitio_id = body.get('sitio_id')     
        prioridad = body.get('prioridad', 'media') # Visitas usa 'media' por defecto si falta
        descripcion = body.get('descripcion', '')  # Visitas lo lee como 'notas'
        
        if not cliente_id or not sitio_id:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "cliente_id y sitio_id son obligatorios"})
            }
            
        # 3. Generar Orden
        # NOTA: Asegúrate de usar clientes reales (ej. CLI-0001) desde el frontend
        orden_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
        timestamp = datetime.utcnow().isoformat()
        
        item = {
            "tenant_id": tenant_id,
            "orden_id": orden_id,
            "cliente_id": cliente_id,
            "sitio_id": sitio_id,
            "prioridad": prioridad,
            "descripcion": descripcion,
            "estado": "CREADA",
            "fecha_creacion": timestamp
        }
        
        # 4. Guardar en Dynamo
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=item)
        
        # 5. Publicar en SNS.
        # Visitas lee orden_id, cliente_id, sitio_id, descripcion, prioridad.
        sns_client.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(item),
            MessageAttributes={
                'evento': {
                    'DataType': 'String',
                    'StringValue': 'orden.creada'
                },
                'tenant_id': {
                    'DataType': 'String',
                    'StringValue': tenant_id
                }
            }
        )
        
        return {
            "statusCode": 201,
            "headers": CORS_HEADERS,
            "body": json.dumps(item)
        }
        
    except ValueError as ve:
        return {"statusCode": 401, "headers": CORS_HEADERS, "body": json.dumps({"error": str(ve)})}
    except Exception as e:
        return {"statusCode": 500, "headers": CORS_HEADERS, "body": json.dumps({"error": str(e)})}

def listar_ordenes(event, context):
    try:
        tenant_id = validar_token(event.get('headers', {}))
        
        query_params = event.get('queryStringParameters') or {}
        limit = int(query_params.get('limit', 50)) 
        next_token = query_params.get('nextToken')
        
        table = dynamodb.Table(TABLE_NAME)
        
        query_kwargs = {
            'KeyConditionExpression': boto3.dynamodb.conditions.Key('tenant_id').eq(tenant_id),
            'Limit': limit
        }
        
        if next_token:
            decoded_key = json.loads(base64.b64decode(next_token).decode('utf-8'))
            query_kwargs['ExclusiveStartKey'] = decoded_key
            
        response = table.query(**query_kwargs)
        items = response.get('Items', [])
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        response_next_token = None
        if last_evaluated_key:
            response_next_token = base64.b64encode(json.dumps(last_evaluated_key).encode('utf-8')).decode('utf-8')
            
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "items": items,
                "nextToken": response_next_token
            })
        }
        
    except ValueError as ve:
        return {"statusCode": 401, "headers": CORS_HEADERS, "body": json.dumps({"error": str(ve)})}
    except Exception as e:
        return {"statusCode": 500, "headers": CORS_HEADERS, "body": json.dumps({"error": str(e)})}
