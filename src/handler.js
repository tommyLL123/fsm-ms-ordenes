const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

const dynamodb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

const ORDENES_TABLE = process.env.ORDENES_TABLE;
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

const getTenantId = (event) => {
    // En produccion, esto vendria del Lambda Authorizer
    const tenantId = event.headers && event.headers['x-tenant-id']; 
    if (!tenantId) throw new Error("Falta el tenant_id en los headers");
    return tenantId;
};

module.exports.crear = async (event) => {
    try {
        const tenant_id = getTenantId(event);
        const data = JSON.parse(event.body);
        const orden_id = uuidv4();

        const orden = {
            tenant_id,
            orden_id,
            cliente_id: data.cliente_id,
            tecnico_id: data.tecnico_id,
            descripcion: data.descripcion,
            estado: 'CREADA',
            fecha_creacion: new Date().toISOString()
        };

        await dynamodb.put({
            TableName: ORDENES_TABLE,
            Item: orden
        }).promise();

        await sns.publish({
            TopicArn: SNS_TOPIC_ARN,
            Message: JSON.stringify(orden),
            MessageAttributes: {
                evento: { DataType: 'String', StringValue: 'orden.creada' },
                tenant_id: { DataType: 'String', StringValue: tenant_id }
            }
        }).promise();

        return {
            statusCode: 201,
            body: JSON.stringify({ message: 'Orden creada y evento publicado al SNS', orden })
        };
    } catch (error) {
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};

module.exports.listar = async (event) => {
    try {
        const tenant_id = getTenantId(event);
        const params = {
            TableName: ORDENES_TABLE,
            KeyConditionExpression: 'tenant_id = :t',
            ExpressionAttributeValues: { ':t': tenant_id }
        };
        const result = await dynamodb.query(params).promise();
        return { statusCode: 200, body: JSON.stringify(result.Items) };
    } catch (error) {
        return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
    }
};
