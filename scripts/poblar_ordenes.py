import argparse
import json
import random
from datetime import datetime, timedelta

DESCRIPCIONES = [
    "Mantenimiento preventivo de aire acondicionado", "Revisión de tablero eléctrico",
    "Instalación de cámara de seguridad", "Reparación de bomba de agua",
    "Cambio de filtros del sistema", "Diagnóstico de falla en equipo",
    "Calibración de sensores", "Reemplazo de cableado dañado",
]
ESTADOS = ["CREADA", "CREADA", "EN_PROCESO", "COMPLETADA"]
BASE = datetime(2026, 9, 1, 8, 0, 0)


def generar_orden(tenant, i, clientes, rnd):
    cliente = rnd.randint(1, clientes)
    return {
        "tenant_id": tenant,
        "orden_id": f"ORD-{i:06d}",
        "cliente_id": f"CLI-{cliente:04d}",
        "sitio_id": f"SIT-{(cliente - 1) * 2 + rnd.randint(1, 2):04d}",
        "prioridad": rnd.choice(["baja", "media", "media", "alta"]),
        "descripcion": rnd.choice(DESCRIPCIONES),
        "estado": rnd.choice(ESTADOS),
        "fecha_creacion": (BASE + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stage", default="dev")
    p.add_argument("--tenants", default="tenant-1,tenant-2,tenant-3")
    p.add_argument("--por-tenant", type=int, default=10000)
    p.add_argument("--clientes", type=int, default=2500, help="clientes por tenant (5000 sitios en MS-A)")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    tenants = [t.strip() for t in a.tenants.split(",") if t.strip()]

    if a.dry_run:
        print(json.dumps(generar_orden(tenants[0], 1, a.clientes, random.Random(tenants[0])),
                         ensure_ascii=False, indent=2))
        print(f"Total de órdenes que se cargarían: {len(tenants) * a.por_tenant}")
        return

    import boto3
    tabla = boto3.resource("dynamodb").Table(f"fsm-{a.stage}-ordenes")
    total = 0
    for t in tenants:
        rnd = random.Random(t)
        with tabla.batch_writer(overwrite_by_pkeys=["tenant_id", "orden_id"]) as bw:
            for i in range(1, a.por_tenant + 1):
                bw.put_item(Item=generar_orden(t, i, a.clientes, rnd))
        total += a.por_tenant
        print(f"{t}: {a.por_tenant} órdenes")
    print(f"Listo: {total} órdenes cargadas en fsm-{a.stage}-ordenes")


if __name__ == "__main__":
    main()
