const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

// Asegurate de tener tus credenciales de AWS configuradas localmente
AWS.config.update({ region: 'us-east-1' });
const dynamodb = new AWS.DynamoDB.DocumentClient();

// Reemplaza con el nombre exacto de tu tabla desplegada
const TABLE_NAME = 'ordenes-dev'; 

const tenants = ['tenant1', 'tenant2', 'tenant3'];
const TOTAL_RECORDS = 30000;
const BATCH_SIZE = 25; // Limite maximo de batchWrite en DynamoDB

async function generarDatos() {
    let records = [];
    console.log(`Generando ${TOTAL_RECORDS} registros ficticios...`);
    
    for (let i = 0; i < TOTAL_RECORDS; i++) {
        records.push({
            tenant_id: tenants[i % 3],
            orden_id: uuidv4(),
            cliente_id: `cliente-${Math.floor(Math.random() * 1000)}`,
            tecnico_id: `tecnico-${Math.floor(Math.random() * 100)}`,
            descripcion: `Mantenimiento o instalacion #${i}`,
            estado: 'CREADA',
            fecha_creacion: new Date().toISOString()
        });
    }

    console.log(`Insertando en DynamoDB en lotes de ${BATCH_SIZE}...`);
    
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);
        const params = {
            RequestItems: {
                [TABLE_NAME]: batch.map(item => ({
                    PutRequest: { Item: item }
                }))
            }
        };
        
        try {
            await dynamodb.batchWrite(params).promise();
            if ((i + BATCH_SIZE) % 1000 === 0) {
                console.log(`Progreso: ${i + BATCH_SIZE} / ${TOTAL_RECORDS}`);
            }
        } catch (error) {
            console.error('Error insertando lote:', error);
        }
    }
    console.log('¡Generacion de datos completada exitosamente!');
}

generarDatos();
