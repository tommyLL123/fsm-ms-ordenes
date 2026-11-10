import base64
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')

TABLE_NAME = os.environ['ORDENES_TABLE']
TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
VALIDAR_TOKEN_FUNC = os.environ['USUARIOS_VALIDAR_LAMBDA']

ROLES_GESTION = {'admin', 'despachador'}

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
}


class ErrorHttp(Exception):
    def __init__(self, status, mensaje):
        super().__init__(mensaje)
        self.status = status
        self.mensaje = mensaje


def respuesta(status, body):
    return {"statusCode": status, "headers": CORS_HEADERS, "body": json.dumps(body, default=str)}


def validar_token(headers):
    headers_lower = {k.lower(): v for k, v in (headers or {}).items()}
    auth_header = headers_lower.get('authorization', '')

    if not auth_header.startswith('Bearer '):
        raise ErrorHttp(401, "Falta el token o formato inválido")

    token = auth_header.split(' ', 1)[1].strip()

    try:
        response = lambda_client.invoke(
            FunctionName=VALIDAR_TOKEN_FUNC,
            InvocationType='RequestResponse',
            Payload=json.dumps({"token": token})
        )
        payload = json.loads(response['Payload'].read())
    except Exception as e:
        print(f"Error al invocar validar-token: {e}")
        raise ErrorHttp(500, "No se pudo validar el token")

    if payload.get('statusCode') != 200:
        raise ErrorHttp(401, "Token inválido o expirado")

    body = payload.get('body', {})
    if isinstance(body, str):
        body = json.loads(body)

    if not body.get('tenant_id'):
        raise ErrorHttp(401, "El token no trae tenant_id")

    return body


def crear_orden(event, context):
    try:
        usuario = validar_token(event.get('headers', {}))
        tenant_id = usuario['tenant_id']

        if usuario.get('rol') not in ROLES_GESTION:
            raise ErrorHttp(403, "Tu rol no puede crear órdenes")

        try:
            body = json.loads(event.get('body') or '{}')
        except json.JSONDecodeError:
            raise ErrorHttp(400, "El cuerpo no contiene un JSON válido")
        if not isinstance(body, dict):
            raise ErrorHttp(400, "El cuerpo debe ser un objeto JSON")

        cliente_id = body.get('cliente_id')
        sitio_id = body.get('sitio_id')
        prioridad = body.get('prioridad', 'media')
        descripcion = body.get('descripcion', '')

        if not cliente_id or not sitio_id:
            raise ErrorHttp(400, "cliente_id y sitio_id son obligatorios")

        orden_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=item)

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

        return respuesta(201, item)

    except ErrorHttp as e:
        return respuesta(e.status, {"error": e.mensaje})
    except Exception as e:
        print(f"Error en crear_orden: {e}")
        return respuesta(500, {"error": "Error interno"})


def listar_ordenes(event, context):
    try:
        tenant_id = validar_token(event.get('headers', {}))['tenant_id']

        query_params = event.get('queryStringParameters') or {}
        try:
            limit = max(1, min(int(query_params.get('limit', 50)), 100))
        except ValueError:
            raise ErrorHttp(400, "limit debe ser un número entero")
        next_token = query_params.get('nextToken')

        table = dynamodb.Table(TABLE_NAME)

        query_kwargs = {
            'KeyConditionExpression': Key('tenant_id').eq(tenant_id),
            'Limit': limit
        }

        if next_token:
            try:
                decoded_key = json.loads(base64.b64decode(next_token).decode('utf-8'))
            except Exception:
                raise ErrorHttp(400, "nextToken no es válido")
            decoded_key['tenant_id'] = tenant_id
            query_kwargs['ExclusiveStartKey'] = decoded_key

        response = table.query(**query_kwargs)
        items = response.get('Items', [])

        last_evaluated_key = response.get('LastEvaluatedKey')
        response_next_token = None
        if last_evaluated_key:
            response_next_token = base64.b64encode(json.dumps(last_evaluated_key).encode('utf-8')).decode('utf-8')

        return respuesta(200, {"items": items, "nextToken": response_next_token})

    except ErrorHttp as e:
        return respuesta(e.status, {"error": e.mensaje})
    except Exception as e:
        print(f"Error en listar_ordenes: {e}")
        return respuesta(500, {"error": "Error interno"})


def obtener_orden(event, context):
    try:
        tenant_id = validar_token(event.get('headers', {}))['tenant_id']
        orden_id = (event.get('pathParameters') or {}).get('orden_id')
        if not orden_id:
            raise ErrorHttp(400, "Falta orden_id")

        item = dynamodb.Table(TABLE_NAME).get_item(
            Key={'tenant_id': tenant_id, 'orden_id': orden_id}
        ).get('Item')
        if not item:
            raise ErrorHttp(404, "Orden no encontrada")

        return respuesta(200, item)

    except ErrorHttp as e:
        return respuesta(e.status, {"error": e.mensaje})
    except Exception as e:
        print(f"Error en obtener_orden: {e}")
        return respuesta(500, {"error": "Error interno"})
