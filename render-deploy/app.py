from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuración
GEMINI_API_KEY = "AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k"
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

# Configurar credenciales
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

genai.configure(api_key=GEMINI_API_KEY)
bq_client = bigquery.Client(project=PROJECT_ID)

# CAMPOS NUMÉRICOS (solo estos pueden usar AVG, SUM)
NUMERIC_FIELDS = """
ID, Cantidad_de_asignaciones, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, 
Tiempo_de_Abordaje__Segundos_, Segundos_Sin_Asignar, Tickets_fusionados,
Tiempo_asignado_sin_abordaje__Segundos_, Tiempo_de_abordaje_ejecutivo__Segundos_
"""

# CAMPOS DE TEXTO (solo COUNT, COUNT DISTINCT)
TEXT_FIELDS = """
Fecha_de_inicio, Hora_de_inicio, Estado, Canal, Nick_del_Cliente, Departamento,
Texto_del_Primer_Mensaje, Texto_del_ultimo_Mensaje, Tipificaciones, 
Tipificacion_Bot, Menu_inicial, Sentimiento_Inicial, Identifier, Empresa, Grupo
"""

# CAMPOS BOOLEANOS (true/false, 1/0)
BOOLEAN_FIELDS = """
Tiene_mensajes_publicos, Tiene_mensajes_privados, Tiene_ticket_previo, Respondido,
Importante, Abordado, Abordado_en_SLA, Tipificado, Escalado, Proactivo,
Creado_en_horario_habil, Abordado_en_SLA_ejecutivo, Primera_asignacion_humana
"""

@app.route('/')
def home():
    return {"status": "Backend Adereso - Gemini + Tipos de Datos Correctos"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]},
        "tickets": []
    })

def generate_dynamic_sql(user_query):
    """Genera SQL con tipos de datos correctos"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Genera SQL para BigQuery en la tabla `{TABLE_ID}` basada en: "{user_query}"

    CAMPOS NUMÉRICOS (puedes usar AVG, SUM, MAX, MIN):
    {NUMERIC_FIELDS}

    CAMPOS DE TEXTO (solo usa COUNT, COUNT DISTINCT):
    {TEXT_FIELDS}

    CAMPOS BOOLEANOS (usa = 'true' o = 'false'):
    {BOOLEAN_FIELDS}

    EJEMPLOS CORRECTOS:
    - "promedio de mensajes" → SELECT AVG(Mensajes) as promedio FROM tabla
    - "tiempo promedio de abordaje" → SELECT AVG(Tiempo_de_Abordaje__Segundos_) as promedio FROM tabla
    - "tickets por canal" → SELECT Canal, COUNT(*) as cantidad FROM tabla GROUP BY Canal
    - "mensajes iniciales" → SELECT Identifier, Texto_del_Primer_Mensaje FROM tabla LIMIT 20
    - "tickets escalados" → SELECT * FROM tabla WHERE Escalado = 'true'
    - "sentimientos" → SELECT Sentimiento_Inicial, COUNT(*) as cantidad FROM tabla GROUP BY Sentimiento_Inicial

    REGLAS CRÍTICAS:
    1. NUNCA uses AVG() en campos de texto como Canal, Estado, Tipificaciones
    2. Solo usa AVG(), SUM() en campos numéricos listados arriba
    3. Para campos de texto usa COUNT(*), COUNT(DISTINCT campo)
    4. Para campos booleanos usa = 'true' o = 'false'
    5. Para fechas usa campos como STRING, no DATETIME
    6. Siempre incluye LIMIT (máximo 100)

    IMPORTANTE: Solo devuelve la consulta SQL sin explicaciones.

    SQL:
    """
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validación adicional: verificar que no use AVG en campos incorrectos
        if 'AVG(' in sql.upper():
            # Verificar que solo use AVG en campos numéricos
            numeric_list = [field.strip() for field in NUMERIC_FIELDS.split(',')]
            for field in numeric_list:
                if field and f'AVG({field})' in sql:
                    continue  # OK
            # Si llegamos aquí y aún tiene AVG, podría ser problemático
            print(f"Advertencia: SQL contiene AVG: {sql}")
        
        # Validación de seguridad
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql.upper() for keyword in dangerous_keywords):
            return None
        
        return sql
        
    except Exception as e:
        print(f"Error generando SQL: {e}")
        return None

def generate_chart_with_identifiers(results):
    """Genera gráfico usando Identifiers cuando sea posible"""
    if len(results) == 0:
        return None
    
    # Para datos agregados (con 'cantidad')
    if 'cantidad' in results.columns:
        return {
            "labels": results.iloc[:, 0].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15]
        }
    
    # Para promedios
    if 'promedio' in results.columns:
        return {
            "labels": ["Promedio"],
            "values": [float(results['promedio'].iloc[0])]
        }
    
    # Para conteos totales
    if 'total' in results.columns:
        return {
            "labels": ["Total"],
            "values": [int(results['total'].iloc[0])]
        }
    
    # Para tickets individuales con Identifier
    if 'Identifier' in results.columns:
        identifiers = results['Identifier'].head(20).fillna('Sin ID').tolist()
        
        # Usar Mensajes como valor si está disponible
        if 'Mensajes' in results.columns:
            return {
                "labels": identifiers,
                "values": results['Mensajes'].head(20).fillna(0).tolist()
            }
        # Usar tiempos de abordaje si están disponibles
        elif 'Tiempo_de_Abordaje__Segundos_' in results.columns:
            return {
                "labels": identifiers,
                "values": results['Tiempo_de_Abordaje__Segundos_'].head(20).fillna(0).tolist()
            }
        else:
            return {
                "labels": identifiers,
                "values": list(range(1, len(identifiers) + 1))
            }
    
    # Fallback
    return {
        "labels": ["Registros"],
        "values": [len(results)]
    }

def should_show_tickets(user_query, results):
    """Determina si mostrar tarjetas de tickets"""
    query_lower = user_query.lower()
    
    # Mostrar tickets para consultas de detalle
    show_conditions = [
        'últimos' in query_lower,
        'recientes' in query_lower,
        'mostrar' in query_lower,
        'ver' in query_lower,
        'mensaje' in query_lower and ('inicial' in query_lower or 'final' in query_lower),
        'escalado' in query_lower,
        'fusionado' in query_lower
    ]
    
    # No mostrar para agregaciones
    aggregate_conditions = [
        'total' in query_lower,
        'cuántos' in query_lower,
        'promedio' in query_lower,
        'por canal' in query_lower,
        'por estado' in query_lower
    ]
    
    return any(show_conditions) and not any(aggregate_conditions) and len(results) <= 25

def generate_tickets_data(results, user_query):
    """Genera tarjetas de tickets con todos los campos relevantes"""
    if not should_show_tickets(user_query, results):
        return []
    
    tickets = []
    for _, row in results.head(15).iterrows():
        ticket = {}
        
        # Campos principales
        if 'ID' in row and pd.notna(row['ID']):
            ticket['id'] = str(row['ID'])
        if 'Identifier' in row and pd.notna(row['Identifier']):
            ticket['identifier'] = str(row['Identifier'])
        if 'Canal' in row and pd.notna(row['Canal']):
            ticket['canal'] = str(row['Canal'])
        if 'Estado' in row and pd.notna(row['Estado']):
            ticket['estado'] = str(row['Estado'])
        if 'Departamento' in row and pd.notna(row['Departamento']):
            ticket['departamento'] = str(row['Departamento'])
        
        # Mensajes
        if 'Mensajes' in row and pd.notna(row['Mensajes']):
            ticket['mensajes'] = str(row['Mensajes'])
        if 'Texto_del_Primer_Mensaje' in row and pd.notna(row['Texto_del_Primer_Mensaje']):
            ticket['primer_mensaje'] = str(row['Texto_del_Primer_Mensaje'])[:100] + "..."
        if 'Texto_del_ultimo_Mensaje' in row and pd.notna(row['Texto_del_ultimo_Mensaje']):
            ticket['ultimo_mensaje'] = str(row['Texto_del_ultimo_Mensaje'])[:100] + "..."
        
        # Tipificaciones
        if 'Tipificaciones' in row and pd.notna(row['Tipificaciones']):
            ticket['tipificaciones'] = str(row['Tipificaciones'])
        if 'Tipificacion_Bot' in row and pd.notna(row['Tipificacion_Bot']):
            ticket['tipificacion_bot'] = str(row['Tipificacion_Bot'])
        if 'Menu_inicial' in row and pd.notna(row['Menu_inicial']):
            ticket['menu_inicial'] = str(row['Menu_inicial'])
        
        # Sentimientos y estados
        if 'Sentimiento_Inicial' in row and pd.notna(row['Sentimiento_Inicial']):
            ticket['sentimiento'] = str(row['Sentimiento_Inicial'])
        if 'Escalado' in row and pd.notna(row['Escalado']):
            ticket['escalado'] = str(row['Escalado'])
        
        if len(ticket) > 1:
            tickets.append(ticket)
    
    return tickets

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{current_time}] Consulta: {user_query}")
        
        # Generar SQL con tipos correctos
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude generar una consulta SQL válida. ¿Puedes reformular tu pregunta?",
                "chart": None,
                "tickets": []
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta
        results = bq_client.query(sql).to_dataframe()
        
        print(f"Registros obtenidos: {len(results)} a las {current_time}")
        
        # Generar respuesta, gráfico y tickets
        chart_data = generate_chart_with_identifiers(results)
        tickets_data = generate_tickets_data(results, user_query)
        
        # Respuesta con Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        data_sample = results.head(10).to_string() if len(results) > 0 else "No hay datos"
        
        response_prompt = f"""
        CONSULTA: "{user_query}"
        DATOS: {data_sample}
        
        Responde como analista de Adereso. Sé específico con números y patrones. Usa emojis.
        """
        
        response = model.generate_content(response_prompt)
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "tickets": tickets_data,
            "data_count": len(results),
            "timestamp": current_time
        })
        
    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_time}] Error: {str(e)}")
        return jsonify({
            "text": f"Error consultando datos: {str(e)}",
            "chart": None,
            "tickets": [],
            "timestamp": error_time
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
